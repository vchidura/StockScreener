"""Read-only verification of stored option discovery evidence.

`option_discovery_repository` only writes (persist and read back its own rows). The V2 options-cycle input
(`option_cycle_input`) references a discovery batch by id and hashes, so something must re-resolve those
references against `rt.option_discovery_batches`, `rt.option_discovery_coverage` and `rt.option_discovery_pages`
without rewriting any repository behavior. This module does that with three plain SELECTs per call, whatever the
number of batches, and never fetches a page body: only ids, digests, counts and byte lengths cross the wire.

It also defines the two digests the cycle input carries, so the builder (from in-memory evidence) and the verifier
(from stored rows) cannot disagree:
- `coverage_sha256`: the eight included/excluded boundary rows (`rt.option_discovery_coverage`),
- `payload_chain_sha256`: the ordered page payload hashes (`rt.option_discovery_pages.payload_sha256`).

Page bodies live in a 14-day HOT_TTL partitioned table; the batch and coverage rows are permanent. A verifier
therefore reports pages as pruned (not as a mismatch) when no page row remains, unless the caller requires them.
Reads use a tuple-row cursor, like every repository in this package (the COMPUTE role covers these reads).
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from .contracts import canonical_sha256
from .option_discovery_repository import OptionDiscoveryCoverageBoundary

DISCOVERY_COVERAGE_HASH_VERSION = "option-discovery-coverage-v1"
DISCOVERY_PAYLOAD_CHAIN_VERSION = "option-discovery-payload-chain-v1"


def coverage_sha256(boundaries: Iterable[OptionDiscoveryCoverageBoundary]) -> str:
    ordered = sorted(boundaries, key=lambda row: (row.dimension, row.disposition, row.comparison))
    return canonical_sha256({
        "version": DISCOVERY_COVERAGE_HASH_VERSION,
        "boundaries": [
            [row.dimension, row.disposition, row.comparison, row.boundary, row.reason_code] for row in ordered
        ],
    })


def payload_chain_sha256(pages: Iterable[tuple[int, str]]) -> str:
    """Order-sensitive digest over (page number, payload hash); a dropped, added, replaced or reordered page changes it."""
    return canonical_sha256({
        "version": DISCOVERY_PAYLOAD_CHAIN_VERSION,
        "pages": [[number, digest] for number, digest in sorted(pages)],
    })


@dataclass(frozen=True, slots=True)
class StoredDiscoveryPage:
    page_number: int
    payload_sha256: str
    response_bytes: int


@dataclass(frozen=True, slots=True)
class StoredDiscoveryBatch:
    discovery_batch_id: UUID
    underlying_security_id: UUID
    profile_id: UUID
    profile_sha256: str
    universe_sha256: str
    provider_tier: str
    underlyer: str
    scheduled_cycle: datetime
    expiration_from: date
    expiration_through: date
    strike_min: Decimal
    strike_max: Decimal
    request_filter_sha256: str
    policy_sha256: str
    page_count: int
    row_count: int
    response_bytes: int
    evidence_sha256: str
    coverage: tuple[OptionDiscoveryCoverageBoundary, ...]
    pages: tuple[StoredDiscoveryPage, ...]

    @property
    def coverage_digest(self) -> str:
        return coverage_sha256(self.coverage)

    @property
    def payload_chain_digest(self) -> str:
        return payload_chain_sha256((page.page_number, page.payload_sha256) for page in self.pages)


def _rows(cursor) -> list[tuple]:
    rows = cursor.fetchall()
    if any(not isinstance(row, (tuple, list)) for row in rows):
        raise TypeError("option discovery verification needs a tuple-row cursor")
    return rows


def read_discovery_batches(cursor, batch_ids: Iterable[UUID]) -> dict[UUID, StoredDiscoveryBatch]:
    """The stored batches among `batch_ids` (absent ids are simply missing from the result)."""
    identities = sorted({str(batch_id) for batch_id in batch_ids})
    if not identities:
        return {}
    cursor.execute(
        "SELECT discovery_batch_id::text, underlying_security_id::text, profile_id::text, profile_sha256, "
        "universe_sha256, provider_tier, underlyer, scheduled_cycle, expiration_from, expiration_through, "
        "strike_min, strike_max, request_filter_sha256, policy_sha256, page_count, row_count, response_bytes, "
        "evidence_sha256 FROM rt.option_discovery_batches WHERE discovery_batch_id = ANY(%s::uuid[])",
        (identities,),
    )
    batch_rows = _rows(cursor)
    cursor.execute(
        "SELECT discovery_batch_id::text, dimension, disposition, comparison, boundary, reason_code "
        "FROM rt.option_discovery_coverage WHERE discovery_batch_id = ANY(%s::uuid[]) "
        "ORDER BY discovery_batch_id, dimension, disposition, comparison",
        (identities,),
    )
    coverage: dict[str, list[OptionDiscoveryCoverageBoundary]] = {}
    for row in _rows(cursor):
        coverage.setdefault(str(row[0]), []).append(OptionDiscoveryCoverageBoundary(*row[1:]))
    cursor.execute(
        "SELECT discovery_batch_id::text, page_number, payload_sha256, octet_length(response_bytes) "
        "FROM rt.option_discovery_pages WHERE discovery_batch_id = ANY(%s::uuid[]) "
        "ORDER BY discovery_batch_id, page_number",
        (identities,),
    )
    pages: dict[str, list[StoredDiscoveryPage]] = {}
    for row in _rows(cursor):
        pages.setdefault(str(row[0]), []).append(StoredDiscoveryPage(int(row[1]), str(row[2]), int(row[3])))
    stored: dict[UUID, StoredDiscoveryBatch] = {}
    for row in batch_rows:
        identity = str(row[0])
        batch = StoredDiscoveryBatch(
            UUID(identity), UUID(str(row[1])), UUID(str(row[2])), str(row[3]), str(row[4]), str(row[5]),
            str(row[6]), row[7], row[8], row[9], Decimal(row[10]), Decimal(row[11]), str(row[12]), str(row[13]),
            int(row[14]), int(row[15]), int(row[16]), str(row[17]),
            tuple(coverage.get(identity, ())), tuple(pages.get(identity, ())),
        )
        stored[batch.discovery_batch_id] = batch
    return stored
