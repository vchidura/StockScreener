from __future__ import annotations

import json
import re
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import execute_batch, execute_values, register_uuid

from equity.domain import EvidenceType

from .contracts import canonical_json, canonical_sha256
from .equity_materialization_adapter import RealtimeEquityMaterializationResult


register_uuid()


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
EQUITY_EVIDENCE_BATCH_PAGE_SIZE = 100
EQUITY_EVIDENCE_BULK_PAGE_SIZE = 500
_TECHNICAL_POINTER_TYPES = frozenset({
    EvidenceType.FEATURE_SNAPSHOT,
    EvidenceType.TRADE_SETUP,
})


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    cohort_id: UUID
    evidence_id: UUID
    evidence_type: EvidenceType
    market_time: datetime
    payload_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.cohort_id, UUID) or not isinstance(self.evidence_id, UUID):
            raise TypeError("evidence reference IDs must be UUIDs")
        if not isinstance(self.evidence_type, EvidenceType):
            raise TypeError("evidence reference type must be an EvidenceType")
        if not _SHA256_PATTERN.fullmatch(self.payload_sha256):
            raise ValueError("evidence reference payload hash must be a lowercase SHA-256")
        object.__setattr__(self, "market_time", _utc(self.market_time, "market_time"))


def _evidence_ref(row) -> EvidenceRef | None:
    if row is None:
        return None
    return EvidenceRef(
        cohort_id=UUID(str(row[0])), evidence_id=UUID(str(row[1])),
        evidence_type=EvidenceType(row[2]), market_time=row[3],
        payload_sha256=row[4],
    )


def resolve_current_evidence_ref_cursor(
    cursor,
    *,
    cohort_id: UUID,
    security_id: UUID,
    interval: str,
    evidence_type: EvidenceType,
    model_variant: str,
) -> EvidenceRef | None:
    if evidence_type not in _TECHNICAL_POINTER_TYPES:
        raise ValueError("current technical evidence type is unsupported")
    if not model_variant.strip():
        raise ValueError("model variant cannot be blank")
    cursor.execute(
        "SELECT evidence.cohort_id, evidence.evidence_id, evidence.evidence_type, "
        "evidence.market_time, evidence.payload_sha256 "
        "FROM rt.equity_evidence_current AS current "
        "JOIN rt.evidence_catalog AS catalog ON "
        "catalog.cohort_id=current.cohort_id AND catalog.evidence_id=current.evidence_id "
        "JOIN rt.equity_evidence AS evidence ON "
        "evidence.cohort_id=catalog.cohort_id AND evidence.evidence_id=catalog.evidence_id "
        "AND evidence.durable_at=catalog.durable_at "
        "AND evidence.interval=current.interval "
        "AND evidence.evidence_type=current.evidence_type "
        "AND evidence.source_version=current.model_variant "
        "AND evidence.market_time=current.market_time "
        "AND evidence.observed_at=current.observed_at "
        "AND evidence.payload_sha256=current.payload_sha256 "
        "AND evidence.quality_state=current.quality_state "
        "AND evidence.valid_until IS NOT DISTINCT FROM current.valid_until "
        "WHERE current.cohort_id=%s AND current.security_id=%s "
        "AND current.interval=%s AND current.evidence_type=%s "
        "AND current.model_variant=%s",
        (
            str(cohort_id), str(security_id), interval,
            evidence_type.value, model_variant,
        ),
    )
    return _evidence_ref(cursor.fetchone())


def resolve_evidence_ref_as_of_cursor(
    cursor,
    *,
    cohort_id: UUID,
    security_id: UUID,
    interval: str,
    evidence_type: EvidenceType,
    model_variant: str,
    market_as_of: datetime,
    known_by: datetime,
) -> EvidenceRef | None:
    if evidence_type not in _TECHNICAL_POINTER_TYPES:
        raise ValueError("as-of technical evidence type is unsupported")
    if not model_variant.strip():
        raise ValueError("model variant cannot be blank")
    cursor.execute(
        "SELECT cohort_id, evidence_id, evidence_type, market_time, payload_sha256 "
        "FROM rt.equity_evidence WHERE cohort_id=%s AND security_id=%s "
        "AND interval=%s AND evidence_type=%s AND source_version=%s "
        "AND market_time<=%s AND observed_at<=%s "
        "ORDER BY market_time DESC, observed_at DESC, evidence_id DESC LIMIT 1",
        (
            str(cohort_id), str(security_id), interval, evidence_type.value,
            model_variant, _utc(market_as_of, "market_as_of"),
            _utc(known_by, "known_by"),
        ),
    )
    return _evidence_ref(cursor.fetchone())


def resolve_release_evidence_refs_cursor(cursor, refs: Sequence[EvidenceRef], *, security_id: UUID,
                                         known_by: datetime) -> Mapping[UUID, EvidenceRef]:
    requested = tuple(refs)
    if not requested:
        return {}
    if len({ref.cohort_id for ref in requested}) != 1 or len({ref.evidence_id for ref in requested}) != len(requested):
        raise ValueError("Release evidence references require one cohort and unique IDs")
    cursor.execute(
        "SELECT evidence.cohort_id, evidence.evidence_id, evidence.evidence_type, "
        "evidence.market_time, evidence.payload_sha256 "
        "FROM rt.evidence_catalog AS catalog "
        "JOIN rt.equity_evidence AS evidence ON "
        "evidence.cohort_id=catalog.cohort_id AND evidence.evidence_id=catalog.evidence_id "
        "AND evidence.durable_at=catalog.durable_at "
        "WHERE catalog.cohort_id=%s AND evidence.security_id=%s "
        "AND catalog.evidence_id=ANY(%s::uuid[]) AND evidence.observed_at<=%s "
        "AND catalog.durable_at<=%s",
        (str(requested[0].cohort_id), str(security_id), [str(ref.evidence_id) for ref in requested],
         _utc(known_by, "known_by"), _utc(known_by, "known_by")),
    )
    selected = {}
    for row in cursor.fetchall():
        values = tuple(row[key] for key in ("cohort_id", "evidence_id", "evidence_type", "market_time", "payload_sha256")) if isinstance(row, Mapping) else row
        ref = _evidence_ref(values)
        selected[ref.evidence_id] = ref
    if selected != {ref.evidence_id: ref for ref in requested}:
        raise ValueError("Release-selected evidence is missing or its immutable identity/hash changed")
    return selected


_RELEASE_EVIDENCE_COLUMNS = (
    "evidence_id", "evidence_type", "evidence_role", "market_time", "observed_at", "valid_until",
    "source_name", "source_version", "payload_schema_version", "analysis_run_id", "latest_bar_revision_id",
    "security_revision_id", "fundamental_report_ids", "quality_state", "quality_codes", "payload",
    "payload_sha256", "source_window_role", "primary_bar_revision_ids", "confirmation_bar_revision_ids",
)
_RELEASE_EVIDENCE_MISMATCH = "Release-selected evidence is missing or its immutable identity/hash changed"


def _fetched_rows(cursor, columns: Sequence[str]) -> list[Mapping]:
    return [row if isinstance(row, Mapping) else dict(zip(columns, row)) for row in cursor.fetchall()]


def load_release_evidence_bodies_cursor(
    cursor, expected: Mapping[UUID, tuple[str, str]], *, cohort_id: UUID, security_id: UUID,
    provider_tier: str, known_by: datetime,
) -> dict[UUID, dict]:
    """Full, verified bodies of the evidence a release pinned (`expected`: id -> (type, payload hash)).

    Every pinned row must still resolve as of `known_by` with its type and payload hash, and its payload
    must hash to the stored digest again; anything else raises the same error as
    `resolve_release_evidence_refs_cursor`. The stored `source_revision_ids` column is empty by design (the
    lineage lives in the source window and the dependency rows), so it is rebuilt from them: the window's
    primary or confirmation bars for a window role, the ordered dependencies for a derived row, else none."""
    if not expected:
        return {}
    known = _utc(known_by, "known_by")
    cursor.execute(
        "SELECT evidence.evidence_id AS evidence_id, evidence.evidence_type AS evidence_type, "
        "evidence.evidence_role AS evidence_role, evidence.market_time AS market_time, "
        "evidence.observed_at AS observed_at, evidence.valid_until AS valid_until, "
        "evidence.source_name AS source_name, evidence.source_version AS source_version, "
        "evidence.payload_schema_version AS payload_schema_version, "
        "evidence.analysis_run_id AS analysis_run_id, "
        "evidence.latest_bar_revision_id AS latest_bar_revision_id, "
        "evidence.security_revision_id AS security_revision_id, "
        "evidence.fundamental_report_ids AS fundamental_report_ids, "
        "evidence.quality_state AS quality_state, evidence.quality_codes AS quality_codes, "
        "evidence.payload AS payload, evidence.payload_sha256 AS payload_sha256, "
        "evidence.source_window_role AS source_window_role, "
        "source_window.primary_bar_revision_ids AS primary_bar_revision_ids, "
        "source_window.confirmation_bar_revision_ids AS confirmation_bar_revision_ids "
        "FROM rt.evidence_catalog AS catalog "
        "JOIN rt.equity_evidence AS evidence ON "
        "evidence.cohort_id=catalog.cohort_id AND evidence.evidence_id=catalog.evidence_id "
        "AND evidence.durable_at=catalog.durable_at "
        "LEFT JOIN rt.evidence_source_window_catalog AS window_catalog ON "
        "window_catalog.cohort_id=evidence.cohort_id AND window_catalog.source_window_id=evidence.source_window_id "
        "LEFT JOIN rt.equity_evidence_source_windows AS source_window ON "
        "source_window.cohort_id=window_catalog.cohort_id "
        "AND source_window.source_window_id=window_catalog.source_window_id "
        "AND source_window.durable_at=window_catalog.durable_at "
        "WHERE catalog.cohort_id=%s AND evidence.provider_tier=%s AND evidence.security_id=%s "
        "AND catalog.evidence_id=ANY(%s::uuid[]) AND evidence.observed_at<=%s AND catalog.durable_at<=%s",
        (str(cohort_id), provider_tier, str(security_id), [str(value) for value in expected], known, known),
    )
    rows = {UUID(str(row["evidence_id"])): row for row in _fetched_rows(cursor, _RELEASE_EVIDENCE_COLUMNS)}
    if rows.keys() != expected.keys() or any(
        (row["evidence_type"], row["payload_sha256"]) != expected[evidence_id]
        or canonical_sha256(row["payload"]) != row["payload_sha256"]
        for evidence_id, row in rows.items()
    ):
        raise ValueError(_RELEASE_EVIDENCE_MISMATCH)
    derived = [evidence_id for evidence_id, row in rows.items() if row["source_window_role"] is None]
    dependencies: dict[UUID, list[UUID]] = {}
    if derived:
        cursor.execute(
            "SELECT dependency.evidence_id AS evidence_id, dependency.dependency_order AS dependency_order, "
            "dependency.dependency_evidence_id AS dependency_evidence_id "
            "FROM rt.equity_evidence_dependencies AS dependency "
            "JOIN rt.evidence_catalog AS catalog ON catalog.cohort_id=dependency.cohort_id "
            "AND catalog.evidence_id=dependency.evidence_id AND catalog.durable_at=dependency.durable_at "
            "WHERE dependency.cohort_id=%s AND dependency.evidence_id=ANY(%s::uuid[]) "
            "ORDER BY dependency.evidence_id, dependency.dependency_order",
            (str(cohort_id), [str(value) for value in derived]),
        )
        ordered: dict[UUID, list[tuple[int, UUID]]] = {}
        for row in _fetched_rows(cursor, ("evidence_id", "dependency_order", "dependency_evidence_id")):
            ordered.setdefault(UUID(str(row["evidence_id"])), []).append(
                (row["dependency_order"], UUID(str(row["dependency_evidence_id"])))
            )
        for evidence_id, edges in ordered.items():
            if [order for order, _ in edges] != list(range(len(edges))):
                raise ValueError(_RELEASE_EVIDENCE_MISMATCH)
            dependencies[evidence_id] = [value for _, value in edges]
    bodies: dict[UUID, dict] = {}
    for evidence_id, row in rows.items():
        role = row["source_window_role"]
        if role == "PRIMARY":
            sources = row["primary_bar_revision_ids"]
        elif role == "CONFIRMATION":
            sources = row["confirmation_bar_revision_ids"]
        else:
            sources = dependencies.get(evidence_id, ())
        if sources is None:
            raise ValueError(_RELEASE_EVIDENCE_MISMATCH)
        bodies[evidence_id] = dict(
            evidence_id=evidence_id, evidence_type=row["evidence_type"], payload_sha256=row["payload_sha256"],
            payload=row["payload"], source_revision_ids=tuple(sources), market_time=row["market_time"],
            observed_at=row["observed_at"], quality_codes=row["quality_codes"],
            analysis_run_id=row["analysis_run_id"], security_revision_id=row["security_revision_id"],
            latest_bar_revision_id=row["latest_bar_revision_id"], source_name=row["source_name"],
            source_version=row["source_version"], payload_schema_version=row["payload_schema_version"],
            valid_until=row["valid_until"], quality_state=row["quality_state"],
            evidence_role=row["evidence_role"], fundamental_report_ids=row["fundamental_report_ids"],
        )
    return bodies


@dataclass(frozen=True, slots=True)
class RealtimeFeatureDefinition:
    bundle_sha256: str
    bundle_version: str
    feature_schema_version: str
    dependency_spec: Mapping[str, object]
    policy_sha256: str
    code_sha256: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.bundle_sha256, "bundle_sha256"),
            (self.policy_sha256, "policy_sha256"),
            (self.code_sha256, "code_sha256"),
        ):
            if not _SHA256_PATTERN.fullmatch(value):
                raise ValueError(f"{name} must be a lowercase SHA-256")
        if not self.bundle_version.strip() or not self.feature_schema_version.strip():
            raise ValueError("feature definition versions cannot be blank")
        canonical_json(self.dependency_spec)


@dataclass(frozen=True, slots=True)
class RealtimeEquityMaterializationWrite:
    result: RealtimeEquityMaterializationResult
    definition: RealtimeFeatureDefinition
    generation: int
    update_evidence_current: bool = True


@dataclass(frozen=True, slots=True)
class RealtimeEquityBatchReceipt:
    persisted_snapshot_ids: tuple[UUID, ...]
    skipped_source_window_ids: tuple[UUID, ...]

    @property
    def persisted_count(self) -> int:
        return len(self.persisted_snapshot_ids)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped_source_window_ids)


@dataclass(frozen=True, slots=True)
class _PreparedRealtimeEquityMaterialization:
    result: RealtimeEquityMaterializationResult
    definition: RealtimeFeatureDefinition
    generation: int
    evidence_rows: tuple
    feature: object
    primary_ids: tuple[UUID, ...]
    confirmation_ids: tuple[UUID, ...]
    source_window_id: UUID
    expected_window: tuple
    input_watermarks: Mapping[str, object]
    state_sha256: str
    snapshot_id: UUID
    expected_definition: tuple
    source_roles: Mapping[UUID, str | None]
    dependency_rows: tuple
    expected_evidence: Mapping[str, tuple]
    expected_dependencies: Mapping[tuple[str, int], str]
    expected_snapshot: tuple
    update_evidence_current: bool = True


def _prepare_realtime_equity_materialization(
    result: RealtimeEquityMaterializationResult,
    definition: RealtimeFeatureDefinition,
    *,
    generation: int,
    update_evidence_current: bool = True,
) -> _PreparedRealtimeEquityMaterialization:
    if type(update_evidence_current) is not bool:
        raise TypeError("evidence discovery pointer policy must be boolean")
    if type(generation) is not int or generation <= 0:
        raise ValueError("feature generation must be a positive integer")
    evidence_rows = tuple(result.materialization.evidence)
    if not evidence_rows:
        raise ValueError("realtime equity materialization has no evidence")
    feature_rows = tuple(
        row for row in evidence_rows
        if row.evidence_type is EvidenceType.FEATURE_SNAPSHOT
    )
    if len(feature_rows) != 1:
        raise ValueError("realtime equity materialization requires one feature snapshot")
    feature = feature_rows[0]
    if feature.source_version != definition.bundle_version:
        raise ValueError("feature definition version does not match materialized evidence")
    if any(row.security_id != feature.security_id for row in evidence_rows):
        raise ValueError("realtime equity evidence cannot mix securities")

    primary_ids = tuple(result.bar_revision_ids)
    confirmation_ids = tuple(result.confirmation_bar_revision_ids)
    all_bar_ids = set(primary_ids) | set(confirmation_ids)
    if any(
        row.latest_bar_revision_id is not None
        and row.latest_bar_revision_id not in all_bar_ids
        for row in evidence_rows
    ):
        raise ValueError("equity evidence latest bar is outside adapter lineage")

    source_window_id = uuid5(
        NAMESPACE_URL,
        f"realtime-equity-evidence-window:{result.cohort_id}:{result.input_sha256}",
    )
    expected_window = (
        str(result.cohort_id), str(source_window_id), result.provider_tier.value,
        str(feature.security_id), result.instrument_id, result.interval,
        feature.market_time, feature.observed_at, primary_ids, confirmation_ids,
        result.input_sha256,
    )
    input_watermarks = {
        "source_window_id": source_window_id,
        "source_window_sha256": result.input_sha256,
    }
    state_sha256 = canonical_sha256({
        "bundle_sha256": definition.bundle_sha256,
        "payload_sha256": feature.payload_sha256,
        "input_watermarks": input_watermarks,
    })
    snapshot_id = uuid5(
        NAMESPACE_URL,
        ":".join((
            "realtime-feature",
            str(result.cohort_id),
            str(result.instrument_id),
            definition.bundle_sha256,
            str(generation),
            state_sha256,
        )),
    )
    expected_definition = (
        definition.bundle_sha256,
        definition.bundle_version,
        definition.feature_schema_version,
        definition.policy_sha256,
        definition.code_sha256,
    )
    evidence_ids = {row.evidence_id for row in evidence_rows}
    source_roles = {}
    dependency_rows = []
    for row in evidence_rows:
        source_ids = tuple(row.source_revision_ids)
        if not source_ids:
            source_roles[row.evidence_id] = None
        elif source_ids == primary_ids:
            source_roles[row.evidence_id] = "PRIMARY"
        elif source_ids == confirmation_ids:
            source_roles[row.evidence_id] = "CONFIRMATION"
        elif all(value in evidence_ids for value in source_ids):
            source_roles[row.evidence_id] = None
            dependency_rows.extend(
                (
                    str(result.cohort_id), str(row.evidence_id), index,
                    str(dependency_id),
                )
                for index, dependency_id in enumerate(source_ids)
            )
        else:
            raise ValueError("realtime equity evidence has unsupported source lineage")
    dependency_values = tuple(dependency_rows)
    return _PreparedRealtimeEquityMaterialization(
        result=result,
        definition=definition,
        generation=generation,
        evidence_rows=evidence_rows,
        feature=feature,
        primary_ids=primary_ids,
        confirmation_ids=confirmation_ids,
        source_window_id=source_window_id,
        expected_window=expected_window,
        input_watermarks=input_watermarks,
        state_sha256=state_sha256,
        snapshot_id=snapshot_id,
        expected_definition=expected_definition,
        source_roles=source_roles,
        dependency_rows=dependency_values,
        expected_evidence={
            str(row.evidence_id): (
                row.evidence_key, row.payload_sha256, str(source_window_id),
                source_roles[row.evidence_id],
            )
            for row in evidence_rows
        },
        expected_dependencies={
            (row[1], row[2]): row[3] for row in dependency_values
        },
        expected_snapshot=(
            str(snapshot_id), str(result.cohort_id), result.provider_tier.value,
            result.instrument_id, definition.bundle_sha256, generation, state_sha256,
        ),
        update_evidence_current=update_evidence_current,
    )


def _persist_realtime_equity_materialization(
    connection,
    result: RealtimeEquityMaterializationResult,
    definition: RealtimeFeatureDefinition,
    *,
    repository_cursor,
    generation: int,
    values_executor=execute_values,
    batch_executor=execute_batch,
) -> UUID:
    prepared = _prepare_realtime_equity_materialization(
        result, definition, generation=generation,
    )
    evidence_rows = prepared.evidence_rows
    feature = prepared.feature
    state_sha256 = prepared.state_sha256
    snapshot_id = prepared.snapshot_id

    if (connection is None) == (repository_cursor is None):
        raise ValueError("exactly one connection or repository cursor is required")
    try:
        cursor_context = (
            connection.cursor()
            if connection is not None
            else nullcontext(repository_cursor)
        )
        with cursor_context as cursor:
            _write_prepared_closures(
                cursor, (prepared,), values_executor=values_executor,
                page_size=EQUITY_EVIDENCE_BATCH_PAGE_SIZE,
            )

            cursor.execute(
                "INSERT INTO rt.feature_current ("
                "cohort_id, provider_tier, instrument_id, bundle_sha256, generation, "
                "snapshot_id, source_cutoff, computed_at, valid_until, quality, state_sha256"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (cohort_id, instrument_id, bundle_sha256) DO UPDATE SET "
                "provider_tier=EXCLUDED.provider_tier, generation=EXCLUDED.generation, "
                "snapshot_id=EXCLUDED.snapshot_id, source_cutoff=EXCLUDED.source_cutoff, "
                "computed_at=EXCLUDED.computed_at, valid_until=EXCLUDED.valid_until, "
                "quality=EXCLUDED.quality, state_sha256=EXCLUDED.state_sha256, "
                "updated_at=clock_timestamp() "
                "WHERE rt.feature_current.provider_tier=EXCLUDED.provider_tier "
                "AND rt.feature_current.generation < EXCLUDED.generation",
                (
                    str(result.cohort_id), result.provider_tier.value,
                    result.instrument_id, definition.bundle_sha256, generation,
                    str(snapshot_id), feature.market_time, feature.observed_at,
                    feature.valid_until, feature.quality_state.value, state_sha256,
                ),
            )
            current_rows = [
                (
                    str(result.cohort_id), result.provider_tier.value,
                    str(row.security_id), result.instrument_id, row.interval,
                    row.evidence_type.value, row.source_version, row.lifecycle_key,
                    str(row.evidence_id), row.market_time, row.observed_at,
                    row.payload_sha256, row.quality_state.value, row.valid_until,
                )
                for row in evidence_rows
                if (
                    row.lifecycle_key
                    and row.interval
                    and row.evidence_type in _TECHNICAL_POINTER_TYPES
                )
            ]
            if current_rows:
                batch_executor(
                    cursor,
                    "INSERT INTO rt.equity_evidence_current ("
                    "cohort_id, provider_tier, security_id, instrument_id, interval, "
                    "evidence_type, model_variant, lifecycle_key, evidence_id, market_time, "
                    "observed_at, payload_sha256, quality_state, valid_until) VALUES ("
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (cohort_id, security_id, interval, evidence_type, model_variant) "
                    "WHERE interval IS NOT NULL AND evidence_type IN ("
                    "'FEATURE_SNAPSHOT', 'TRADE_SETUP') DO UPDATE SET "
                    "provider_tier=EXCLUDED.provider_tier, instrument_id=EXCLUDED.instrument_id, "
                    "lifecycle_key=EXCLUDED.lifecycle_key, "
                    "evidence_id=EXCLUDED.evidence_id, market_time=EXCLUDED.market_time, "
                    "observed_at=EXCLUDED.observed_at, payload_sha256=EXCLUDED.payload_sha256, "
                    "quality_state=EXCLUDED.quality_state, valid_until=EXCLUDED.valid_until, "
                    "updated_at=clock_timestamp() "
                    "WHERE rt.equity_evidence_current.provider_tier=EXCLUDED.provider_tier "
                    "AND (rt.equity_evidence_current.market_time < EXCLUDED.market_time "
                    "OR (rt.equity_evidence_current.market_time=EXCLUDED.market_time "
                    "AND rt.equity_evidence_current.observed_at < EXCLUDED.observed_at))",
                    current_rows,
                    page_size=EQUITY_EVIDENCE_BATCH_PAGE_SIZE,
                )
        if connection is not None:
            connection.commit()
    except Exception:
        if connection is not None:
            connection.rollback()
        raise
    return snapshot_id


def persist_realtime_equity_materialization_cursor(
    cursor,
    result: RealtimeEquityMaterializationResult,
    definition: RealtimeFeatureDefinition,
    *,
    generation: int,
    values_executor=execute_values,
    batch_executor=execute_batch,
) -> UUID:
    return _persist_realtime_equity_materialization(
        None,
        result,
        definition,
        repository_cursor=cursor,
        generation=generation,
        values_executor=values_executor,
        batch_executor=batch_executor,
    )


def persist_realtime_equity_materialization(
    connection,
    result: RealtimeEquityMaterializationResult,
    definition: RealtimeFeatureDefinition,
    *,
    generation: int,
    values_executor=execute_values,
    batch_executor=execute_batch,
) -> UUID:
    return _persist_realtime_equity_materialization(
        connection,
        result,
        definition,
        repository_cursor=None,
        generation=generation,
        values_executor=values_executor,
        batch_executor=batch_executor,
    )


_EVIDENCE_COLUMNS = (
    "cohort_id, provider_tier, evidence_id, evidence_key, lifecycle_key, "
    "evidence_type, evidence_role, security_id, instrument_id, ticker, "
    "interval, direction, lifecycle_status, strength, market_time, observed_at, "
    "valid_until, source_name, source_version, payload_schema_version, "
    "analysis_run_id, latest_bar_revision_id, security_revision_id, "
    "fundamental_report_ids, source_revision_ids, quality_state, quality_codes, "
    "qualification_revision_id, payload, payload_sha256, "
    "materialization_input_sha256, source_window_id, source_window_role"
)
_VERIFY_EVIDENCE_CHUNK = 2_000


def _stored_source_window(row) -> tuple:
    return tuple(
        str(value) if isinstance(value, UUID) else value for value in row[:8]
    ) + (tuple(row[8]), tuple(row[9]), row[10])


def _write_feature_definitions(cursor, pending, *, values_executor, page_size) -> None:
    definition_values = {}
    for row in pending:
        definition_values.setdefault(
            row.definition.bundle_sha256,
            (
                *row.expected_definition[:3],
                canonical_json(row.definition.dependency_spec),
                *row.expected_definition[3:],
            ),
        )
    values_executor(
        cursor,
        "INSERT INTO rt.feature_definitions ("
        "bundle_sha256, bundle_version, feature_schema_version, dependency_spec, "
        "policy_sha256, code_sha256) VALUES %s "
        "ON CONFLICT (bundle_sha256) DO NOTHING",
        tuple(definition_values.values()),
        page_size=page_size,
    )
    cursor.execute(
        "SELECT bundle_sha256, bundle_version, feature_schema_version, "
        "policy_sha256, code_sha256 FROM rt.feature_definitions "
        "WHERE bundle_sha256=ANY(%s::text[])",
        (list(definition_values),),
    )
    stored_definitions = {row[0]: tuple(row) for row in cursor.fetchall()}
    expected_definitions = {
        row.definition.bundle_sha256: row.expected_definition for row in pending
    }
    if stored_definitions != expected_definitions:
        raise ValueError("persisted feature definition identity conflict")


def _write_source_windows(
    cursor, pending, cohort_id: str, *, values_executor, page_size,
) -> dict[str, datetime]:
    values_executor(
        cursor,
        "INSERT INTO rt.evidence_source_window_catalog ("
        "cohort_id, source_window_id, source_window_sha256, interval, market_time"
        ") VALUES %s ON CONFLICT DO NOTHING",
        tuple(
            (row.expected_window[0], row.expected_window[1], row.expected_window[10],
             row.expected_window[5], row.expected_window[6])
            for row in pending
        ),
        page_size=page_size,
    )
    expected_catalog = {
        str(row.source_window_id): (
            row.expected_window[10], row.expected_window[5], row.expected_window[6],
        )
        for row in pending
    }
    cursor.execute(
        "SELECT source_window_id, source_window_sha256, interval, market_time, durable_at "
        "FROM rt.evidence_source_window_catalog "
        "WHERE cohort_id=%s AND source_window_id=ANY(%s::uuid[])",
        (cohort_id, list(expected_catalog)),
    )
    catalog_rows = cursor.fetchall()
    if {str(row[0]): tuple(row[1:4]) for row in catalog_rows} != expected_catalog:
        raise ValueError("persisted equity evidence source window conflict")
    durable = {str(row[0]): row[4] for row in catalog_rows}
    values_executor(
        cursor,
        "INSERT INTO rt.equity_evidence_source_windows ("
        "cohort_id, source_window_id, provider_tier, security_id, instrument_id, "
        "interval, market_time, observed_at, primary_bar_revision_ids, "
        "confirmation_bar_revision_ids, source_window_sha256, durable_at) VALUES %s "
        "ON CONFLICT DO NOTHING",
        tuple(
            (*_source_window_values(row), durable[str(row.source_window_id)])
            for row in pending
        ),
        page_size=page_size,
    )
    cursor.execute(
        "SELECT cohort_id, source_window_id, provider_tier, security_id, "
        "instrument_id, interval, market_time, observed_at, "
        "primary_bar_revision_ids, confirmation_bar_revision_ids, "
        "source_window_sha256 FROM rt.equity_evidence_source_windows "
        "WHERE cohort_id=%s AND source_window_id=ANY(%s::uuid[]) "
        "AND durable_at=ANY(%s::timestamptz[])",
        (cohort_id, list(expected_catalog), list(set(durable.values()))),
    )
    persisted = {str(row[1]): _stored_source_window(row) for row in cursor.fetchall()}
    if persisted != {str(row.source_window_id): row.expected_window for row in pending}:
        raise ValueError("persisted equity evidence source window conflict")
    return durable


def _write_evidence(
    cursor, pending, cohort_id: str, *, values_executor, page_size,
) -> dict[str, datetime]:
    catalog_values = tuple(
        (
            cohort_id, str(row.evidence_id), row.evidence_key,
            prepared.result.provider_tier.value, row.evidence_type.value, row.interval,
            str(row.security_id), prepared.result.instrument_id, row.market_time,
        )
        for prepared in pending
        for row in prepared.evidence_rows
    )
    values_executor(
        cursor,
        "INSERT INTO rt.evidence_catalog ("
        "cohort_id, evidence_id, evidence_key, provider_tier, evidence_type, interval, "
        "security_id, instrument_id, market_time) VALUES %s ON CONFLICT DO NOTHING",
        catalog_values,
        page_size=page_size,
    )
    evidence_ids = [value[1] for value in catalog_values]
    cursor.execute(
        "SELECT evidence_id, evidence_key, durable_at FROM rt.evidence_catalog "
        "WHERE cohort_id=%s AND evidence_id=ANY(%s::uuid[])",
        (cohort_id, evidence_ids),
    )
    stored = {str(row[0]): (row[1], row[2]) for row in cursor.fetchall()}
    if {key: value[0] for key, value in stored.items()} != {
        value[1]: value[2] for value in catalog_values
    }:
        raise ValueError("persisted equity evidence identity conflict")
    durable = {key: value[1] for key, value in stored.items()}
    values_executor(
        cursor,
        f"INSERT INTO rt.equity_evidence ({_EVIDENCE_COLUMNS}, durable_at) "
        "VALUES %s ON CONFLICT DO NOTHING",
        tuple(
            (*value, durable[value[2]])
            for prepared in pending
            for value in _evidence_values(prepared)
        ),
        page_size=page_size,
    )
    cursor.execute(
        "SELECT evidence_id, evidence_key, payload_sha256, source_window_id, "
        "source_window_role FROM rt.equity_evidence WHERE cohort_id=%s "
        "AND evidence_id=ANY(%s::uuid[]) AND durable_at=ANY(%s::timestamptz[])",
        (cohort_id, evidence_ids, list(set(durable.values()))),
    )
    persisted = {
        str(row[0]): (row[1], row[2], str(row[3]), row[4]) for row in cursor.fetchall()
    }
    if persisted != {
        evidence_id: identity
        for prepared in pending
        for evidence_id, identity in prepared.expected_evidence.items()
    }:
        raise ValueError("persisted equity evidence identity conflict")
    return durable


def _write_dependencies(
    cursor, pending, cohort_id: str, durable: Mapping[str, datetime], *,
    values_executor, page_size,
) -> None:
    dependency_values = tuple(
        (*value, durable[value[1]]) for prepared in pending for value in prepared.dependency_rows
    )
    if dependency_values:
        values_executor(
            cursor,
            "INSERT INTO rt.equity_evidence_dependencies ("
            "cohort_id, evidence_id, dependency_order, dependency_evidence_id, durable_at) "
            "VALUES %s ON CONFLICT DO NOTHING",
            dependency_values,
            page_size=page_size,
        )
    cursor.execute(
        "SELECT evidence_id, dependency_order, dependency_evidence_id "
        "FROM rt.equity_evidence_dependencies WHERE cohort_id=%s "
        "AND evidence_id=ANY(%s::uuid[]) AND durable_at=ANY(%s::timestamptz[])",
        (cohort_id, list(durable), list(set(durable.values()))),
    )
    persisted = {(str(row[0]), row[1]): str(row[2]) for row in cursor.fetchall()}
    if persisted != {
        identity: dependency
        for prepared in pending
        for identity, dependency in prepared.expected_dependencies.items()
    }:
        raise ValueError("persisted equity evidence dependency conflict")


def _write_snapshots(
    cursor, pending, durable: Mapping[str, datetime], *, values_executor, page_size,
) -> None:
    snapshot_ids = [str(row.snapshot_id) for row in pending]
    expected = {str(row.snapshot_id): row.expected_snapshot for row in pending}
    select = (
        "SELECT snapshot_id, cohort_id, provider_tier, instrument_id, "
        "bundle_sha256, generation, state_sha256 FROM rt.feature_snapshots "
        "WHERE snapshot_id=ANY(%s::uuid[])"
    )

    def stored() -> dict[str, tuple]:
        cursor.execute(select, (snapshot_ids,))
        return {
            str(row[0]): tuple(
                str(value) if isinstance(value, UUID) else value for value in row
            )
            for row in cursor.fetchall()
        }

    existing = stored()
    if any(existing[key] != expected[key] for key in existing if key in expected):
        raise ValueError("persisted feature snapshot identity conflict")
    missing = tuple(row for row in pending if str(row.snapshot_id) not in existing)
    if missing:
        # A snapshot lives in the partition of the evidence it was computed from.
        values_executor(
            cursor,
            "INSERT INTO rt.feature_snapshots ("
            "snapshot_id, cohort_id, provider_tier, instrument_id, bundle_sha256, "
            "generation, source_cutoff, computed_at, valid_until, quality, state_sha256, "
            "input_watermarks, source_evidence_id, durable_at) VALUES %s "
            "ON CONFLICT DO NOTHING",
            tuple(
                (*_snapshot_values(row), durable[str(row.feature.evidence_id)])
                for row in missing
            ),
            page_size=page_size,
        )
        existing = stored()
    if existing != expected:
        raise ValueError("persisted feature snapshot identity conflict")


def _write_prepared_closures(cursor, pending, *, values_executor, page_size) -> None:
    """Write definitions, windows, evidence, dependencies and snapshots for pending closures.

    Catalog rows are inserted first (they own idempotency); bodies reuse the catalog's
    durable_at so a replay can never create a second body in another monthly partition.
    """
    cohort_id = str(pending[0].result.cohort_id)
    _write_feature_definitions(
        cursor, pending, values_executor=values_executor, page_size=page_size,
    )
    _write_source_windows(
        cursor, pending, cohort_id, values_executor=values_executor, page_size=page_size,
    )
    durable = _write_evidence(
        cursor, pending, cohort_id, values_executor=values_executor, page_size=page_size,
    )
    _write_dependencies(
        cursor, pending, cohort_id, durable,
        values_executor=values_executor, page_size=page_size,
    )
    _write_snapshots(
        cursor, pending, durable, values_executor=values_executor, page_size=page_size,
    )


def _evidence_values(prepared: _PreparedRealtimeEquityMaterialization) -> tuple:
    result = prepared.result
    return tuple(
        (
            str(result.cohort_id), result.provider_tier.value,
            str(row.evidence_id), row.evidence_key, row.lifecycle_key,
            row.evidence_type.value, row.evidence_role.value,
            str(row.security_id), result.instrument_id, row.ticker,
            row.interval, row.direction, row.lifecycle_status.value, row.strength,
            row.market_time, row.observed_at, row.valid_until, row.source_name,
            row.source_version, row.payload_schema_version,
            str(row.analysis_run_id) if row.analysis_run_id else None,
            str(row.latest_bar_revision_id) if row.latest_bar_revision_id else None,
            str(row.security_revision_id) if row.security_revision_id else None,
            list(row.fundamental_report_ids), [], row.quality_state.value,
            list(row.quality_codes),
            str(row.qualification_revision_id)
            if row.qualification_revision_id else None,
            row.payload_json, row.payload_sha256, result.input_sha256,
            str(prepared.source_window_id), prepared.source_roles[row.evidence_id],
        )
        for row in prepared.evidence_rows
    )


def _source_window_values(
    prepared: _PreparedRealtimeEquityMaterialization,
) -> tuple:
    values = prepared.expected_window
    return (*values[:8], list(values[8]), list(values[9]), values[10])


def _snapshot_values(prepared: _PreparedRealtimeEquityMaterialization) -> tuple:
    result = prepared.result
    feature = prepared.feature
    return (
        str(prepared.snapshot_id), str(result.cohort_id), result.provider_tier.value,
        result.instrument_id, prepared.definition.bundle_sha256,
        prepared.generation, feature.market_time, feature.observed_at,
        feature.valid_until, feature.quality_state.value, prepared.state_sha256,
        canonical_json(prepared.input_watermarks), str(feature.evidence_id),
    )


def _feature_current_values(
    prepared: _PreparedRealtimeEquityMaterialization,
) -> tuple:
    result = prepared.result
    feature = prepared.feature
    return (
        str(result.cohort_id), result.provider_tier.value, result.instrument_id,
        prepared.definition.bundle_sha256, prepared.generation,
        str(prepared.snapshot_id), feature.market_time, feature.observed_at,
        feature.valid_until, feature.quality_state.value, prepared.state_sha256,
    )


def _evidence_current_values(
    prepared: _PreparedRealtimeEquityMaterialization,
) -> tuple:
    if not prepared.update_evidence_current:
        return ()
    result = prepared.result
    return tuple(
        (
            str(result.cohort_id), result.provider_tier.value,
            str(row.security_id), result.instrument_id, row.interval,
            row.evidence_type.value, row.source_version, row.lifecycle_key,
            str(row.evidence_id), row.market_time, row.observed_at,
            row.payload_sha256, row.quality_state.value, row.valid_until,
        )
        for row in prepared.evidence_rows
        if (
            row.lifecycle_key
            and row.interval
            and row.evidence_type in _TECHNICAL_POINTER_TYPES
        )
    )


def _latest_feature_current_values(prepared_rows):
    latest = {}
    for prepared in prepared_rows:
        key = (prepared.result.cohort_id, prepared.result.instrument_id, prepared.definition.bundle_sha256)
        previous = latest.get(key)
        if previous is not None and (previous.result.provider_tier != prepared.result.provider_tier
                or previous.generation == prepared.generation and previous.snapshot_id != prepared.snapshot_id):
            raise ValueError("feature current batch has conflicting tier/generation identities")
        if previous is None or prepared.generation > previous.generation:
            latest[key] = prepared
    return tuple(_feature_current_values(prepared) for prepared in latest.values())


def verify_realtime_equity_materializations_persisted_cursor(
    cursor, writes: Sequence[RealtimeEquityMaterializationWrite],
) -> int:
    """Read-only: raise ValueError unless every evidence row the writes imply is already persisted with the
    identity (key, payload digest, source window and role) the writes compute. Returns the evidence rows
    checked. It is the check a recovery makes before it publishes snapshots that name this evidence without
    having computed (and so persisted) it in the same process."""
    requested = tuple(writes)
    prepared = tuple(
        _prepare_realtime_equity_materialization(
            row.result, row.definition, generation=row.generation,
            update_evidence_current=row.update_evidence_current,
        )
        for row in requested
    )
    if not prepared:
        return 0
    cohort_ids = {row.result.cohort_id for row in prepared}
    if len(cohort_ids) != 1:
        raise ValueError("realtime equity materialization verification cannot mix cohorts")
    cohort_id = str(next(iter(cohort_ids)))
    expected = {}
    for row in prepared:
        expected.update(row.expected_evidence)
    evidence_ids = sorted(expected)
    persisted = {}
    for offset in range(0, len(evidence_ids), _VERIFY_EVIDENCE_CHUNK):
        cursor.execute(
            "SELECT evidence_id, evidence_key, payload_sha256, source_window_id, source_window_role "
            "FROM rt.equity_evidence WHERE cohort_id=%s AND evidence_id=ANY(%s::uuid[])",
            (cohort_id, evidence_ids[offset:offset + _VERIFY_EVIDENCE_CHUNK]),
        )
        for stored in cursor.fetchall():
            if isinstance(stored, Mapping):
                stored = (stored["evidence_id"], stored["evidence_key"], stored["payload_sha256"],
                          stored["source_window_id"], stored["source_window_role"])
            persisted[str(stored[0])] = (stored[1], stored[2], str(stored[3]), stored[4])
    if persisted != expected:
        raise ValueError("realtime equity evidence is not persisted as the materializations require")
    return len(expected)


def persist_realtime_equity_materialization_batch_cursor(
    cursor,
    writes: Sequence[RealtimeEquityMaterializationWrite],
    *,
    values_executor=execute_values,
) -> RealtimeEquityBatchReceipt:
    requested = tuple(writes)
    if not requested:
        return RealtimeEquityBatchReceipt((), ())
    if any(not isinstance(row, RealtimeEquityMaterializationWrite) for row in requested):
        raise TypeError("realtime equity materialization batch contains an invalid write")
    prepared = tuple(
        _prepare_realtime_equity_materialization(
            row.result, row.definition, generation=row.generation,
            update_evidence_current=row.update_evidence_current,
        )
        for row in requested
    )
    cohort_ids = {row.result.cohort_id for row in prepared}
    if len(cohort_ids) != 1:
        raise ValueError("realtime equity materialization batch cannot mix cohorts")
    cohort_id = str(next(iter(cohort_ids)))
    window_ids = [str(row.source_window_id) for row in prepared]
    if len(window_ids) != len(set(window_ids)):
        raise ValueError("realtime equity materialization batch has duplicate source windows")
    all_evidence_ids = [
        str(evidence.evidence_id)
        for row in prepared
        for evidence in row.evidence_rows
    ]
    if len(all_evidence_ids) != len(set(all_evidence_ids)):
        raise ValueError("realtime equity materialization batch has duplicate evidence IDs")

    cursor.execute(
        "SELECT sw.cohort_id, sw.source_window_id, sw.provider_tier, "
        "sw.security_id, sw.instrument_id, sw.interval, sw.market_time, "
        "sw.observed_at, sw.primary_bar_revision_ids, "
        "sw.confirmation_bar_revision_ids, sw.source_window_sha256 "
        "FROM rt.evidence_source_window_catalog AS catalog "
        "JOIN rt.equity_evidence_source_windows AS sw ON "
        "sw.cohort_id=catalog.cohort_id "
        "AND sw.source_window_id=catalog.source_window_id "
        "AND sw.durable_at=catalog.durable_at "
        "WHERE catalog.cohort_id=%s AND catalog.source_window_id=ANY(%s::uuid[])",
        (cohort_id, window_ids),
    )
    stored_windows = {
        str(row[1]): tuple(
            str(value) if isinstance(value, UUID) else value for value in row[:8]
        ) + (tuple(row[8]), tuple(row[9]), row[10])
        for row in cursor.fetchall()
    }
    expected_windows = {
        str(row.source_window_id): row.expected_window for row in prepared
    }
    if any(
        stored_windows[key] != expected_windows[key]
        for key in stored_windows
        if key in expected_windows
    ):
        raise ValueError("persisted equity evidence source window conflict")

    cursor.execute(
        "SELECT evidence.evidence_id, evidence.evidence_key, evidence.payload_sha256, "
        "evidence.source_window_id, evidence.source_window_role "
        "FROM rt.evidence_catalog AS catalog "
        "JOIN rt.equity_evidence AS evidence ON "
        "evidence.cohort_id=catalog.cohort_id AND evidence.evidence_id=catalog.evidence_id "
        "AND evidence.durable_at=catalog.durable_at "
        "WHERE catalog.cohort_id=%s AND catalog.evidence_id=ANY(%s::uuid[])",
        (cohort_id, all_evidence_ids),
    )
    stored_evidence = {
        str(row[0]): (row[1], row[2], str(row[3]), row[4])
        for row in cursor.fetchall()
    }
    expected_evidence = {
        evidence_id: identity
        for row in prepared
        for evidence_id, identity in row.expected_evidence.items()
    }
    if any(
        stored_evidence[key] != expected_evidence[key]
        for key in stored_evidence
        if key in expected_evidence
    ):
        raise ValueError("persisted equity evidence identity conflict")

    cursor.execute(
        "SELECT dep.evidence_id, dep.dependency_order, dep.dependency_evidence_id "
        "FROM rt.evidence_catalog AS catalog "
        "JOIN rt.equity_evidence_dependencies AS dep ON "
        "dep.cohort_id=catalog.cohort_id AND dep.evidence_id=catalog.evidence_id "
        "AND dep.durable_at=catalog.durable_at "
        "WHERE catalog.cohort_id=%s AND catalog.evidence_id=ANY(%s::uuid[])",
        (cohort_id, all_evidence_ids),
    )
    stored_dependencies = {
        (str(row[0]), row[1]): str(row[2]) for row in cursor.fetchall()
    }
    expected_dependencies = {
        identity: dependency
        for row in prepared
        for identity, dependency in row.expected_dependencies.items()
    }
    if any(
        stored_dependencies[key] != expected_dependencies.get(key)
        for key in stored_dependencies
    ):
        raise ValueError("persisted equity evidence dependency conflict")

    pending = []
    skipped = []
    for row in prepared:
        evidence_complete = all(
            evidence_id in stored_evidence
            for evidence_id in row.expected_evidence
        )
        dependencies_complete = all(
            identity in stored_dependencies
            for identity in row.expected_dependencies
        )
        window_complete = str(row.source_window_id) in stored_windows
        if window_complete and evidence_complete and dependencies_complete:
            skipped.append(row.source_window_id)
        else:
            pending.append(row)
    if not pending:
        return RealtimeEquityBatchReceipt((), tuple(skipped))

    _write_prepared_closures(
        cursor, tuple(pending), values_executor=values_executor,
        page_size=EQUITY_EVIDENCE_BULK_PAGE_SIZE,
    )

    values_executor(
        cursor,
        "INSERT INTO rt.feature_current ("
        "cohort_id, provider_tier, instrument_id, bundle_sha256, generation, "
        "snapshot_id, source_cutoff, computed_at, valid_until, quality, state_sha256"
        ") VALUES %s ON CONFLICT (cohort_id, instrument_id, bundle_sha256) "
        "DO UPDATE SET provider_tier=EXCLUDED.provider_tier, "
        "generation=EXCLUDED.generation, snapshot_id=EXCLUDED.snapshot_id, "
        "source_cutoff=EXCLUDED.source_cutoff, computed_at=EXCLUDED.computed_at, "
        "valid_until=EXCLUDED.valid_until, quality=EXCLUDED.quality, "
        "state_sha256=EXCLUDED.state_sha256, updated_at=clock_timestamp() "
        "WHERE rt.feature_current.provider_tier=EXCLUDED.provider_tier "
        "AND rt.feature_current.generation < EXCLUDED.generation",
        _latest_feature_current_values(pending),
        page_size=EQUITY_EVIDENCE_BULK_PAGE_SIZE,
    )
    current_values = tuple(
        value for row in pending for value in _evidence_current_values(row)
    )
    if current_values:
        values_executor(
            cursor,
            "INSERT INTO rt.equity_evidence_current ("
            "cohort_id, provider_tier, security_id, instrument_id, interval, "
            "evidence_type, model_variant, lifecycle_key, evidence_id, market_time, "
            "observed_at, payload_sha256, quality_state, valid_until) VALUES %s "
            "ON CONFLICT (cohort_id, security_id, interval, evidence_type, model_variant) "
            "WHERE interval IS NOT NULL AND evidence_type IN ("
            "'FEATURE_SNAPSHOT', 'TRADE_SETUP') DO UPDATE SET "
            "provider_tier=EXCLUDED.provider_tier, instrument_id=EXCLUDED.instrument_id, "
            "lifecycle_key=EXCLUDED.lifecycle_key, evidence_id=EXCLUDED.evidence_id, "
            "market_time=EXCLUDED.market_time, observed_at=EXCLUDED.observed_at, "
            "payload_sha256=EXCLUDED.payload_sha256, "
            "quality_state=EXCLUDED.quality_state, valid_until=EXCLUDED.valid_until, "
            "updated_at=clock_timestamp() "
            "WHERE rt.equity_evidence_current.provider_tier=EXCLUDED.provider_tier "
            "AND (rt.equity_evidence_current.market_time < EXCLUDED.market_time "
            "OR (rt.equity_evidence_current.market_time=EXCLUDED.market_time "
            "AND rt.equity_evidence_current.observed_at < EXCLUDED.observed_at))",
            current_values,
            page_size=EQUITY_EVIDENCE_BULK_PAGE_SIZE,
        )
    return RealtimeEquityBatchReceipt(
        tuple(row.snapshot_id for row in pending), tuple(skipped),
    )