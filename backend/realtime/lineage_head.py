"""Lineage-head manifests: bounded, verifiable provenance for portal snapshots (V2-9).

A snapshot manifest used to embed every source bar row (about 13 MB per ticker) and hash the whole
input set globally. A lineage head replaces the rows with, per (interval, adjustment basis): the
count, the first and last revision, the first and last bar time and an ordered chain hash over
(revision id, payload hash). It stays a few KB however long the history is, so `rt.portal_snapshot_hot`
can enforce its 16 KiB cap and the hash cost no longer scales with retained bars. The rows themselves
are still reachable through `rt.bar_current` and the evidence catalog at this exact head.
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Callable, TypeVar

from .contracts import canonical_json, canonical_sha256

LINEAGE_HEAD_VERSION = "bar_lineage_head_v1"
# Design bar for a hot manifest (the database CHECK is 16 KiB; this leaves headroom).
BOUNDED_MANIFEST_BYTES = 10 * 1024
# A by-reference per-ticker workspace payload is about 60 KB on the nine-name fixture (it was 1.76 MB while
# charts and evidence were embedded); the cap fails a producer that embeds bar or evidence bodies again.
BOUNDED_WORKSPACE_PAYLOAD_BYTES = 128 * 1024

T = TypeVar("T")


def manifest_bytes(manifest: Mapping[str, object]) -> int:
    return len(canonical_json(manifest).encode("ascii"))


def require_bounded_payload(
    payload: Mapping[str, object], *, name: str, limit: int = BOUNDED_WORKSPACE_PAYLOAD_BYTES,
):
    size = len(canonical_json(payload).encode("ascii"))
    if size > limit:
        raise ValueError(f"{name} payload is {size} bytes; a by-reference payload must stay within {limit}")
    return payload


def require_bounded_manifest(manifest: Mapping[str, object], *, name: str, limit: int = BOUNDED_MANIFEST_BYTES):
    size = manifest_bytes(manifest)
    if size > limit:
        raise ValueError(f"{name} manifest is {size} bytes; a lineage-head manifest must stay within {limit}")
    return manifest


# A screening generation is cohort-wide, so its manifest carries one lineage record per member (the daily and
# hourly bar heads, never a per-bar list) next to constants that do not grow: the field, gap and hourly
# contracts, the code hashes and the coverage counters. It was 5.2 MB for nine names (about 226 MB for 400)
# while it listed every source bar; it is now measured in KiB and linear in members only.
BOUNDED_SCREENING_MANIFEST_BASE_BYTES = 24 * 1024
BOUNDED_SCREENING_MANIFEST_MEMBER_BYTES = 3 * 1024


def screening_manifest_limit(members: int) -> int:
    return BOUNDED_SCREENING_MANIFEST_BASE_BYTES + BOUNDED_SCREENING_MANIFEST_MEMBER_BYTES * members


def require_bounded_screening_manifest(manifest: Mapping[str, object], *, members: int, name: str = "screening generation"):
    """Fail a producer (or a writer) whose generation manifest grows with bars instead of members."""
    return require_bounded_manifest(manifest, name=name, limit=screening_manifest_limit(members))


def id_set_identity(ids: Iterable[object]) -> dict[str, object]:
    """Count and digest of a set of identifiers (order independent): what a manifest keeps of an id list."""
    ordered = sorted(str(value) for value in ids)
    return dict(count=len(ordered), sha256=canonical_sha256(ordered))


def revision_chain_sha256(pairs: Iterable[tuple[object, str]]) -> str:
    """Order-sensitive digest over (revision id, payload hash) pairs; a replaced, dropped, added or
    reordered revision changes it."""
    digest = hashlib.sha256()
    segments = [f"{revision_id}:{payload_sha256};" for revision_id, payload_sha256 in pairs]
    digest.update("".join(segments).encode("ascii"))
    return digest.hexdigest()


def _stamp(value: datetime) -> str:
    return value.isoformat()


def group_key(row: Mapping[str, object]) -> str:
    return f"{row['interval']}:{row['adjustment_basis']}"


def group_head(
    *, count: int, first_revision_id, last_revision_id, first_bar_start: datetime, last_bar_end: datetime,
    chain_sha256: str,
) -> dict[str, object]:
    return dict(
        count=count, first_revision_id=str(first_revision_id), last_revision_id=str(last_revision_id),
        first_bar_start=_stamp(first_bar_start), last_bar_end=_stamp(last_bar_end),
        chain_sha256=chain_sha256,
    )


def assemble_lineage_head(groups: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    ordered = {key: groups[key] for key in sorted(groups)}
    head = dict(
        version=LINEAGE_HEAD_VERSION, bars=sum(group["count"] for group in ordered.values()), groups=ordered,
    )
    head["sha256"] = canonical_sha256(ordered)
    return head


def rows_identity(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    """Constant-size identity of a variable-length input list (count and canonical digest)."""
    return dict(count=len(rows), sha256=canonical_sha256(list(rows)))


def mapping_identity(mapping: Mapping[str, object]) -> dict[str, object]:
    """Constant-size identity of a keyed collection (count and canonical digest, key-order independent)."""
    return dict(count=len(mapping), sha256=canonical_sha256(mapping))


def bar_group_head(ordered_rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    """Head of one (interval, basis) group. Rows must already be ordered by bar start."""
    if not ordered_rows:
        raise ValueError("a lineage head needs at least one bar")
    first, last = ordered_rows[0], ordered_rows[-1]
    return group_head(
        count=len(ordered_rows), first_revision_id=first["bar_revision_id"],
        last_revision_id=last["bar_revision_id"], first_bar_start=first["bar_start"],
        last_bar_end=last["bar_end"],
        chain_sha256=revision_chain_sha256(
            (row["bar_revision_id"], row["payload_sha256"]) for row in ordered_rows
        ),
    )


def bar_lineage_head(rows: Iterable[Mapping[str, object]]) -> dict[str, object]:
    """Lineage head of a set of bar rows (any order; ties on bar start break by revision id)."""
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for row in rows:
        grouped.setdefault(group_key(row), []).append(row)
    return assemble_lineage_head({
        key: bar_group_head(sorted(group, key=lambda row: (row["bar_start"], str(row["bar_revision_id"]))))
        for key, group in grouped.items()
    })


def universe_lineage_sha256(heads: Mapping[str, Mapping[str, object]]) -> str:
    """One digest for a set of per-ticker heads: constant size however many tickers there are."""
    return canonical_sha256(sorted((ticker, head["sha256"]) for ticker, head in heads.items()))


def reference_set(members: Iterable[Mapping[str, object]]) -> dict[str, object]:
    """Constant-size identity of a member (reference) list, for aggregate manifests. The list itself
    lives in the payload under `references`; a reader recomputes this from it and compares."""
    ordered = sorted(members, key=lambda row: (row["ticker"], str(row["security_revision_id"])))
    return dict(count=len(ordered), sha256=canonical_sha256(ordered))


def workspace_set(workspaces: Iterable[Mapping[str, object]]) -> dict[str, object]:
    """Constant-size identity of the per-ticker workspace list (ticker, snapshot id, payload hash)."""
    ordered = sorted(workspaces, key=lambda row: row["ticker"])
    return dict(count=len(ordered), sha256=canonical_sha256(ordered))


def require_member_set(identity: object, computed: Mapping[str, object], *, name: str) -> None:
    if identity != computed:
        raise ValueError(f"{name} member-set identity drift")


def memoized_on(instance: object, name: str, factory: Callable[[], T]) -> T:
    """Cache a derived value on a frozen, non-slotted dataclass instance (invalidated by `replace`,
    which builds a new instance)."""
    cache = instance.__dict__.setdefault("_derived_cache", {})
    if name not in cache:
        cache[name] = factory()
    return cache[name]


def inputs_lineage(inputs) -> dict[str, dict[str, object]]:
    """Per-ticker lineage heads of a RealtimeScreeningInput, computed once per input object in one
    pass over the bars (no per-ticker rescan, no global serialization)."""
    def build():
        by_ticker: dict[str, list[Mapping[str, object]]] = {}
        for row in inputs.bars:
            by_ticker.setdefault(row["ticker"], []).append(row)
        return {ticker: bar_lineage_head(rows) for ticker, rows in sorted(by_ticker.items())}
    return memoized_on(inputs, "bar_lineage_heads", build)


def inputs_bar_lineage_sha256(inputs) -> str:
    """Constant-size digest of every ticker's bar lineage head (what aggregate manifests carry instead
    of per-bar revision lists)."""
    return memoized_on(inputs, "bar_lineage_sha256", lambda: universe_lineage_sha256(inputs_lineage(inputs)))


def inputs_sha256(inputs) -> str:
    """Digest of a RealtimeScreeningInput that replaces `canonical_sha256(inputs)`: identity fields,
    members, actions, coverage and the per-ticker lineage heads. Cost is linear in bars with a small
    constant and never serializes a bar row."""
    def build():
        return canonical_sha256(dict(
            cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
            source_manifest_sha256=inputs.source_manifest_sha256, source_generation=inputs.source_generation,
            session=inputs.session, cutoff=inputs.cutoff,
            members=inputs.members, actions=inputs.actions, coverage=inputs.coverage,
            bar_lineage=inputs_bar_lineage_sha256(inputs),
        ))
    return memoized_on(inputs, "inputs_sha256", build)
