from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from uuid import UUID

from psycopg2.extras import Json

from options.data.polygon_chain_discovery import PolygonOptionChainDiscoveryResult

from .contracts import canonical_sha256
from .option_discovery_scope import RealtimeOptionDiscoveryScope


OPTION_DISCOVERY_EVIDENCE_SCHEMA = "realtime_option_discovery_evidence_v1"


@dataclass(frozen=True, slots=True)
class OptionDiscoveryCoverageBoundary:
    dimension: str
    disposition: str
    comparison: str
    boundary: str
    reason_code: str | None


@dataclass(frozen=True, slots=True)
class RealtimeOptionDiscoveryEvidence:
    scope: RealtimeOptionDiscoveryScope
    underlying_security_id: UUID
    result: PolygonOptionChainDiscoveryResult

    def __post_init__(self) -> None:
        batch = self.result.batch
        if batch.provider != "polygon":
            raise ValueError("realtime option discovery requires Polygon evidence")
        if batch.underlyer not in self.scope.universe.option_underlyings:
            raise ValueError("realtime option discovery is outside the reviewed universe")
        if any(
            hashlib.sha256(page.response_bytes).hexdigest() != page.payload_sha256
            for page in batch.pages
        ):
            raise ValueError("option discovery page payload hash mismatch")
        if any(
            page.request_filter_sha256 != batch.request_filter_sha256
            for page in batch.pages
        ):
            raise ValueError("option discovery page filter identity mismatch")
        coverage = self.coverage
        if len(coverage) != 8 or len(set(coverage)) != 8:
            raise ValueError("option discovery requires exact included/excluded coverage")
        expected = {
            ("EXPIRATION_DATE", "INCLUDED", "GTE"),
            ("EXPIRATION_DATE", "INCLUDED", "LTE"),
            ("EXPIRATION_DATE", "EXCLUDED", "LT"),
            ("EXPIRATION_DATE", "EXCLUDED", "GT"),
            ("STRIKE_PRICE", "INCLUDED", "GTE"),
            ("STRIKE_PRICE", "INCLUDED", "LTE"),
            ("STRIKE_PRICE", "EXCLUDED", "LT"),
            ("STRIKE_PRICE", "EXCLUDED", "GT"),
        }
        actual = {
            (row.dimension, row.disposition, row.comparison) for row in coverage
        }
        if actual != expected:
            raise ValueError("option discovery coverage boundaries are incomplete")
        if self.expiration_through < self.expiration_from:
            raise ValueError("option discovery expiration coverage is invalid")
        if self.strike_min <= 0 or self.strike_max < self.strike_min:
            raise ValueError("option discovery strike coverage is invalid")
        if not 1 <= self.maximum_dte <= 365:
            raise ValueError("realtime option discovery DTE horizon is outside bounds")
        if not Decimal("0") < self.strike_corridor_fraction < Decimal("1"):
            raise ValueError("realtime option discovery strike corridor is outside bounds")
        if len(batch.pages) > 40 or batch.row_count > 10_000:
            raise ValueError("option discovery exceeds stable page or row capacity")
        if self.response_bytes > 67_108_864 or any(
            len(page.response_bytes) > 10_485_760 for page in batch.pages
        ):
            raise ValueError("option discovery exceeds stable byte capacity")

    @property
    def coverage(self) -> tuple[OptionDiscoveryCoverageBoundary, ...]:
        included = tuple(
            OptionDiscoveryCoverageBoundary(
                _dimension(row.field),
                "INCLUDED",
                row.comparison.upper(),
                row.boundary,
                None,
            )
            for row in self.result.included_ranges
        )
        excluded = tuple(
            OptionDiscoveryCoverageBoundary(
                _dimension(row.field),
                "EXCLUDED",
                row.comparison.upper(),
                row.boundary,
                (
                    "OUTSIDE_DISCOVERY_WINDOW"
                    if row.field == "expiration_date"
                    else "OUTSIDE_STRIKE_CORRIDOR"
                ),
            )
            for row in self.result.excluded_ranges
        )
        return tuple(sorted(included + excluded, key=lambda row: (
            row.dimension, row.disposition, row.comparison
        )))

    def _boundary(self, dimension: str, comparison: str) -> str:
        matches = [
            row.boundary
            for row in self.coverage
            if row.dimension == dimension
            and row.disposition == "INCLUDED"
            and row.comparison == comparison
        ]
        if len(matches) != 1:
            raise ValueError("option discovery included boundary is not unique")
        return matches[0]

    @property
    def expiration_from(self) -> date:
        return date.fromisoformat(self._boundary("EXPIRATION_DATE", "GTE"))

    @property
    def expiration_through(self) -> date:
        return date.fromisoformat(self._boundary("EXPIRATION_DATE", "LTE"))

    @property
    def strike_min(self) -> Decimal:
        return Decimal(self._boundary("STRIKE_PRICE", "GTE"))

    @property
    def strike_max(self) -> Decimal:
        return Decimal(self._boundary("STRIKE_PRICE", "LTE"))

    @property
    def maximum_dte(self) -> int:
        return (self.expiration_through - self.expiration_from).days

    @property
    def spot_price(self) -> Decimal:
        return (self.strike_min + self.strike_max) / Decimal("2")

    @property
    def strike_corridor_fraction(self) -> Decimal:
        return (self.strike_max - self.strike_min) / (Decimal("2") * self.spot_price)

    @property
    def response_bytes(self) -> int:
        return sum(len(page.response_bytes) for page in self.result.batch.pages)

    @property
    def evidence_sha256(self) -> str:
        batch = self.result.batch
        return canonical_sha256({
            "schema": OPTION_DISCOVERY_EVIDENCE_SCHEMA,
            "scope": self.scope,
            "underlying_security_id": self.underlying_security_id,
            "batch": {
                "batch_id": batch.batch_id,
                "provider": batch.provider,
                "underlyer": batch.underlyer,
                "scheduled_cycle": batch.scheduled_cycle,
                "maximum_dte": self.maximum_dte,
                "spot_price": str(self.spot_price),
                "strike_corridor_fraction": str(self.strike_corridor_fraction),
                "request_filter_sha256": batch.request_filter_sha256,
                "policy_sha256": batch.policy_sha256,
                "started_at": batch.started_at,
                "completed_at": batch.completed_at,
                "page_count": len(batch.pages),
                "row_count": batch.row_count,
                "response_bytes": self.response_bytes,
            },
            "pages": tuple({
                "page_number": page.page_number,
                "row_count": page.row_count,
                "payload_sha256": page.payload_sha256,
                "received_at": page.received_at,
                "terminal": page.terminal,
                "request_filter_sha256": page.request_filter_sha256,
                "request_cursor_sha256": page.request_cursor_sha256,
                "request_id": page.request_id,
                "next_cursor_sha256": page.next_cursor_sha256,
                "request_metadata": page.request_metadata,
            } for page in batch.pages),
            "coverage": self.coverage,
        })


def _dimension(field: str) -> str:
    try:
        return {
            "expiration_date": "EXPIRATION_DATE",
            "strike_price": "STRIKE_PRICE",
        }[field]
    except KeyError as error:
        raise ValueError("option discovery coverage dimension is invalid") from error


def persist_realtime_option_discovery_cursor(
    cursor,
    evidence: RealtimeOptionDiscoveryEvidence,
) -> None:
    batch = evidence.result.batch
    cursor.execute(
        "SELECT profile_sha256, provider_tier, universe_sha256 "
        "FROM rt.capability_profiles WHERE profile_id=%s",
        (str(evidence.scope.profile_id),),
    )
    profile = cursor.fetchone()
    expected_profile = (
        evidence.scope.profile_sha256,
        evidence.scope.provider_tier.value,
        evidence.scope.universe.sha256,
    )
    if profile != expected_profile:
        raise ValueError("option discovery capability profile identity mismatch")
    cursor.execute(
        "SELECT ticker FROM ref.security_revisions WHERE security_id=%s "
        "AND effective_from <= %s AND (effective_to IS NULL OR effective_to > %s) "
        "ORDER BY effective_from DESC LIMIT 1",
        (
            str(evidence.underlying_security_id),
            batch.scheduled_cycle,
            batch.scheduled_cycle,
        ),
    )
    security = cursor.fetchone()
    if security != (batch.underlyer,):
        raise ValueError("option discovery underlying security identity mismatch")
    cursor.execute(
        "INSERT INTO rt.option_discovery_batches ("
        "discovery_batch_id, underlying_security_id, profile_id, profile_sha256, "
        "universe_sha256, provider_tier, provider, underlyer, "
        "scheduled_cycle, expiration_from, expiration_through, maximum_dte, spot_price, "
        "strike_corridor_fraction, strike_min, strike_max, "
        "request_filter_sha256, policy_sha256, started_at, completed_at, page_count, "
        "row_count, response_bytes, evidence_sha256) VALUES ("
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (discovery_batch_id) DO NOTHING",
        (
            str(batch.batch_id), str(evidence.underlying_security_id),
            str(evidence.scope.profile_id), evidence.scope.profile_sha256,
            evidence.scope.universe.sha256, evidence.scope.provider_tier.value,
            batch.provider, batch.underlyer,
            batch.scheduled_cycle, evidence.expiration_from, evidence.expiration_through,
            evidence.maximum_dte, evidence.spot_price,
            evidence.strike_corridor_fraction, evidence.strike_min, evidence.strike_max,
            batch.request_filter_sha256, batch.policy_sha256, batch.started_at,
            batch.completed_at, len(batch.pages), batch.row_count,
            evidence.response_bytes, evidence.evidence_sha256,
        ),
    )
    cursor.execute(
        "SELECT underlying_security_id::text, profile_id::text, profile_sha256, "
        "universe_sha256, provider_tier, evidence_sha256, page_count, row_count, "
        "response_bytes FROM rt.option_discovery_batches WHERE discovery_batch_id=%s",
        (str(batch.batch_id),),
    )
    stored_batch = cursor.fetchone()
    expected_batch = (
        str(evidence.underlying_security_id), str(evidence.scope.profile_id),
        evidence.scope.profile_sha256, evidence.scope.universe.sha256,
        evidence.scope.provider_tier.value, evidence.evidence_sha256,
        len(batch.pages), batch.row_count, evidence.response_bytes,
    )
    if stored_batch != expected_batch:
        raise ValueError("persisted option discovery batch identity conflict")
    for page in batch.pages:
        metadata = dict(page.request_metadata)
        if len(metadata) != len(page.request_metadata):
            raise ValueError("option discovery request metadata has duplicate keys")
        cursor.execute(
            "INSERT INTO rt.option_discovery_pages ("
            "discovery_batch_id, page_number, row_count, response_bytes, payload_sha256, "
            "received_at, terminal, request_filter_sha256, request_cursor_sha256, "
            "request_id, next_cursor_sha256, request_metadata) VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (discovery_batch_id, page_number, received_at) DO NOTHING",
            (
                str(batch.batch_id), page.page_number, page.row_count,
                page.response_bytes, page.payload_sha256, page.received_at, page.terminal,
                page.request_filter_sha256, page.request_cursor_sha256, page.request_id,
                page.next_cursor_sha256, Json(metadata),
            ),
        )
    for row in evidence.coverage:
        cursor.execute(
            "INSERT INTO rt.option_discovery_coverage ("
            "discovery_batch_id, dimension, disposition, comparison, boundary, reason_code) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (
                str(batch.batch_id), row.dimension, row.disposition,
                row.comparison, row.boundary, row.reason_code,
            ),
        )
    cursor.execute(
        "SELECT page_number, row_count, response_bytes, payload_sha256, received_at, "
        "terminal, request_filter_sha256, request_cursor_sha256, request_id, "
        "next_cursor_sha256, request_metadata FROM rt.option_discovery_pages "
        "WHERE discovery_batch_id=%s ORDER BY page_number",
        (str(batch.batch_id),),
    )
    stored_pages = tuple(
        (
            int(row[0]), int(row[1]), bytes(row[2]), row[3], row[4], bool(row[5]),
            row[6], row[7], row[8], row[9], dict(row[10]),
        )
        for row in cursor.fetchall()
    )
    expected_pages = tuple(
        (
            page.page_number, page.row_count, page.response_bytes, page.payload_sha256,
            page.received_at, page.terminal, page.request_filter_sha256,
            page.request_cursor_sha256, page.request_id, page.next_cursor_sha256,
            dict(page.request_metadata),
        )
        for page in batch.pages
    )
    if stored_pages != expected_pages:
        raise ValueError("persisted option discovery pages differ from complete evidence")
    cursor.execute(
        "SELECT dimension, disposition, comparison, boundary, reason_code "
        "FROM rt.option_discovery_coverage WHERE discovery_batch_id=%s "
        "ORDER BY dimension, disposition, comparison",
        (str(batch.batch_id),),
    )
    stored_coverage = tuple(
        OptionDiscoveryCoverageBoundary(*row) for row in cursor.fetchall()
    )
    if stored_coverage != evidence.coverage:
        raise ValueError("persisted option discovery coverage differs from evidence")


def persist_realtime_option_discovery(
    connection,
    evidence: RealtimeOptionDiscoveryEvidence,
) -> None:
    try:
        with connection.cursor() as cursor:
            persist_realtime_option_discovery_cursor(cursor, evidence)
        connection.commit()
    except Exception:
        connection.rollback()
        raise