"""Canonical Equity detector product for receipt-bound V3 Stock Alerts."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.model_detection.codec import decode_input
from equity.model_detection.contracts import (
    Decision,
    Disposition,
    PriorState,
    canonical_sha256 as model_sha256,
    canonical_value,
)
from equity.model_detection.engine import evaluate
from equity.model_detection.policies import (
    BreakoutPolicy,
    ContextOptionalBreakoutPolicy,
)
from research.gics_sectors import (
    SECTOR_BENCHMARK_ETF,
    resolve_benchmark_ticker,
    sector_benchmark_ticker,
)

from .contracts import canonical_sha256
from .detector_adapters import StableDetectorAdapterResult
from .detector_evidence import (
    RealtimeDetectorEvent,
    RealtimeDetectorInput,
    RealtimeDetectorState,
    RealtimeDetectorVersion,
    build_detector_event,
    build_detector_version,
)
from .lineage_head import inputs_sha256
from .portal_projection import RealtimeAlertProjection, build_alert_projection
from .screening_projection import RealtimeScreeningInput


CANONICAL_STOCK_ALERT_PRODUCT = "canonical-equity-stock-alert-v1"
CANONICAL_STOCK_ALERT_SCHEMA = "canonical_equity_stock_alert_v1"
CANONICAL_STOCK_EVALUATION_SCHEMA = "canonical_equity_stock_evaluation_v1"
CANONICAL_STOCK_STATE_SCHEMA = "canonical_equity_stock_state_v1"
CANONICAL_STOCK_CONTEXT_CONTRACT = "canonical-equity-context-v2"
# Earnings and Fed data help but are not required: with none, the event gate reads
# NOT_APPLICABLE and the evaluation continues. A known event still blocks. The user's decision, Nov 5.
CANONICAL_STOCK_ALERT_POLICY = ContextOptionalBreakoutPolicy()
CANONICAL_STOCK_EVENT_LOOKBACK = timedelta(days=1)
CANONICAL_STOCK_EVENT_LOOKAHEAD = timedelta(hours=72)
CANONICAL_STOCK_EVENT_TYPES = (
    "EARNINGS",
    "FED_RATE_DECISION",
)
_SOURCE_ROOT = Path(__file__).resolve().parents[1]
_CODE_PATHS = (
    _SOURCE_ROOT / "equity/model_detection/contracts.py",
    _SOURCE_ROOT / "equity/model_detection/codec.py",
    _SOURCE_ROOT / "equity/model_detection/feature_definitions.py",
    _SOURCE_ROOT / "equity/model_detection/feature_engine.py",
    _SOURCE_ROOT / "equity/model_detection/engine.py",
    _SOURCE_ROOT / "equity/model_detection/policies.py",
    _SOURCE_ROOT / "research/gics_sectors.py",
    Path(__file__),
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("canonical Stock Alert clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal(value) -> str:
    return format(Decimal(str(value)), "f")


def canonical_stock_alert_code_sha256(paths=_CODE_PATHS) -> str:
    digest = hashlib.sha256()
    for path in paths:
        resolved = Path(path)
        if not resolved.is_file():
            raise FileNotFoundError("canonical Stock Alert source is unavailable")
        digest.update(resolved.name.encode("ascii"))
        digest.update(b"\0")
        digest.update(resolved.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def canonical_stock_alert_version(
    policy: BreakoutPolicy = CANONICAL_STOCK_ALERT_POLICY,
) -> RealtimeDetectorVersion:
    return build_detector_version(
        detector_id=CANONICAL_STOCK_ALERT_PRODUCT,
        version="2",
        code_sha256=canonical_stock_alert_code_sha256(),
        policy_sha256=policy.policy_sha256,
        requirements={
            "receipt": "INPUT_READY",
            "bars": {
                "interval": policy.interval,
                "adjustment_basis": "RAW_UNADJUSTED",
                "completed": policy.lookback + 1,
                "maximum_age_seconds": policy.max_age_seconds,
            },
            "context": (
                "market_trend",
                "sector_trend",
                "event_risk",
            ),
            "missing_context": "UNAVAILABLE",
            "optional_context": tuple(
                sorted(policy.optional_context_names)
            ),
            "publication": "MATCH_ONLY",
            "execution_permission": False,
        },
        output_kind="ALERT",
    )


@dataclass(frozen=True, slots=True)
class CanonicalStockAlertContext:
    market_trend: Decimal | None
    sector_trend: Decimal | None
    event_risk: bool | None
    source_record_id: UUID
    source_sha256: str
    available_at: datetime
    received_at: datetime
    evidence: tuple[Mapping[str, object], ...]
    document: Mapping[str, object]
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        available_at = _utc(self.available_at)
        received_at = _utc(self.received_at)
        if received_at < available_at:
            raise ValueError(
                "canonical Stock Alert context receipt precedes availability"
            )
        document = canonical_value(self.document)
        if (
            document.get("contract") != CANONICAL_STOCK_CONTEXT_CONTRACT
            or document.get("market_trend")
            != (
                None
                if self.market_trend is None
                else canonical_value(self.market_trend)
            )
            or document.get("sector_trend")
            != (
                None
                if self.sector_trend is None
                else canonical_value(self.sector_trend)
            )
            or document.get("event_risk") is not self.event_risk
            or tuple(document.get("reason_codes", ())) != self.reason_codes
            or tuple(document.get("evidence", ()))
            != tuple(canonical_value(self.evidence))
            or canonical_sha256(document) != self.source_sha256
            or self.source_record_id
            != uuid5(
                NAMESPACE_URL,
                f"canonical-equity-context:{self.source_sha256}",
            )
        ):
            raise ValueError(
                "canonical Stock Alert context identity changed"
            )
        for item in self.evidence:
            payload_sha256 = str(item.get("payload_sha256", ""))
            if (
                not str(item.get("role", "")).strip()
                or not str(item.get("source_kind", "")).strip()
                or not str(item.get("source_id", "")).strip()
                or len(payload_sha256) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in payload_sha256
                )
            ):
                raise ValueError(
                    "canonical Stock Alert context evidence is invalid"
                )
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "document", document)
        object.__setattr__(
            self,
            "evidence",
            tuple(document["evidence"]),
        )

    @property
    def contributors(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (
                str(item["source_id"]),
                str(item["payload_sha256"]),
            )
            for item in self.evidence
            if item["role"] == "MARKET_BAR"
        )


@dataclass(frozen=True, slots=True)
class CanonicalStockAlertMembership:
    desired_revision_id: UUID | None
    desired_snapshot_sha256: str
    intended_members: tuple[Mapping[str, object], ...]
    effective_members: tuple[Mapping[str, object], ...]

    @property
    def document(self) -> Mapping[str, object]:
        body = {
            "contract": "canonical-stock-alert-membership-v1",
            "desired_revision_id": (
                None
                if self.desired_revision_id is None
                else str(self.desired_revision_id)
            ),
            "desired_snapshot_sha256": self.desired_snapshot_sha256,
            "intended_members": self.intended_members,
            "effective_members": self.effective_members,
            "complete_universe_required": False,
        }
        return {
            **body,
            "membership_sha256": canonical_sha256(body),
        }

    @property
    def membership_sha256(self) -> str:
        return str(self.document["membership_sha256"])

    @property
    def partial(self) -> bool:
        intended = {
            (row["security_id"], row["ticker"])
            for row in self.intended_members
            if row["enabled"]
            and row["status"] == "READY"
            and row["security_id"] is not None
        }
        effective = {
            (row["security_id"], row["ticker"])
            for row in self.effective_members
        }
        return intended != effective or any(
            row["enabled"] and row["status"] != "READY"
            for row in self.intended_members
        )


@dataclass(frozen=True, slots=True)
class CanonicalStockAlertEvaluation:
    security_id: UUID
    instrument_id: int
    ticker: str
    input_sha256: str
    decision: Decision
    event: RealtimeDetectorEvent
    state: RealtimeDetectorState | None
    alert: RealtimeAlertProjection | None


@dataclass(frozen=True, slots=True)
class CanonicalStockAlertBatch:
    cohort_id: UUID
    window_id: UUID
    input_receipt_id: UUID
    session_date: date
    cutoff: datetime
    receipt_input_sha256: str
    detector_input_sha256: str
    membership: CanonicalStockAlertMembership
    version: RealtimeDetectorVersion
    evaluations: tuple[CanonicalStockAlertEvaluation, ...]
    completed_at: datetime
    deadline: datetime | None = None
    is_late: bool = False

    @property
    def status(self) -> str:
        return "COMPLETE"

    @property
    def batch_sha256(self) -> str:
        return canonical_sha256({
            "product": CANONICAL_STOCK_ALERT_PRODUCT,
            "cohort_id": self.cohort_id,
            "window_id": self.window_id,
            "input_receipt_id": self.input_receipt_id,
            "session_date": self.session_date,
            "cutoff": self.cutoff,
            "receipt_input_sha256": self.receipt_input_sha256,
            "detector_input_sha256": self.detector_input_sha256,
            "membership_sha256": self.membership.membership_sha256,
            "detector_sha256": self.version.detector_sha256,
            "events": tuple(row.event.event_id for row in self.evaluations),
            "completed_at": self.completed_at,
            "deadline": self.deadline,
            "is_late": self.is_late,
        })


def canonical_stock_alert_membership(
    inputs: RealtimeScreeningInput,
    *,
    desired_revision_id: UUID | None = None,
    desired_snapshot_sha256: str | None = None,
    intended_members: tuple[Mapping[str, object], ...] | None = None,
) -> CanonicalStockAlertMembership:
    effective = tuple(
        {
            "security_id": str(row["security_id"]),
            "instrument_id": int(row["instrument_id"]),
            "ticker": str(row["ticker"]),
        }
        for row in sorted(
            inputs.members,
            key=lambda value: (
                str(value["ticker"]),
                str(value["security_id"]),
            ),
        )
    )
    if not 1 <= len(effective) <= 400:
        raise ValueError(
            "canonical Stock Alert effective membership must contain 1-400 names"
        )
    if intended_members is None:
        intended = tuple({
            **row,
            "member_kind": "EQUITY",
            "enabled": True,
            "status": "READY",
            "reason_codes": (),
        } for row in effective)
    else:
        intended = tuple(
            {
                "security_id": (
                    None
                    if row.get("security_id") is None
                    else str(row["security_id"])
                ),
                "instrument_id": (
                    None
                    if row.get("instrument_id") is None
                    else int(row["instrument_id"])
                ),
                "ticker": str(row["ticker"]),
                "member_kind": str(row["member_kind"]),
                "enabled": bool(row["enabled"]),
                "status": str(row["status"]),
                "reason_codes": tuple(row.get("reason_codes", ())),
            }
            for row in sorted(
                (
                    row
                    for row in intended_members
                    if str(row["member_kind"]) == "EQUITY"
                ),
                key=lambda value: (
                    str(value["ticker"]),
                    str(value["member_kind"]),
                ),
            )
        )
    if len({(row["ticker"], row["member_kind"]) for row in intended}) != len(
        intended
    ):
        raise ValueError("canonical Stock Alert intended membership is ambiguous")
    if any(
        row["status"] not in {"WARMING", "READY", "FAILED"}
        for row in intended
    ):
        raise ValueError("canonical Stock Alert readiness status is invalid")
    snapshot_sha256 = (
        inputs.source_manifest_sha256
        if desired_snapshot_sha256 is None
        else desired_snapshot_sha256
    )
    if len(snapshot_sha256) != 64 or any(
        value not in "0123456789abcdef" for value in snapshot_sha256
    ):
        raise ValueError("canonical Stock Alert snapshot identity is invalid")
    effective_identities = {
        (row["security_id"], row["ticker"]) for row in effective
    }
    ready_identities = {
        (row["security_id"], row["ticker"])
        for row in intended
        if row["enabled"]
        and row["status"] == "READY"
        and row["security_id"] is not None
    }
    if desired_revision_id is not None:
        effective = tuple(
            row
            for row in effective
            if (row["security_id"], row["ticker"]) in ready_identities
        )
        if not effective:
            raise ValueError(
                "canonical Stock Alert effective membership is unavailable"
            )
        effective_identities = {
            (row["security_id"], row["ticker"]) for row in effective
        }
    if (
        desired_revision_id is not None
        and not effective_identities.issubset(ready_identities)
    ):
        raise ValueError(
            "canonical Stock Alert effective membership changed from its snapshot"
        )
    return CanonicalStockAlertMembership(
        desired_revision_id=desired_revision_id,
        desired_snapshot_sha256=snapshot_sha256,
        intended_members=intended,
        effective_members=effective,
    )


def _product_members(
    inputs: RealtimeScreeningInput,
    membership: CanonicalStockAlertMembership,
) -> tuple[Mapping[str, object], ...]:
    members = {
        str(row["security_id"]): row for row in inputs.members
    }
    selected = []
    for row in membership.effective_members:
        identity = str(row["security_id"])
        member = members.get(identity)
        if (
            member is None
            or str(member["ticker"]) != str(row["ticker"])
            or int(member["instrument_id"]) != int(row["instrument_id"])
        ):
            raise ValueError(
                "canonical Stock Alert effective member input is unavailable"
            )
        selected.append(member)
    return tuple(selected)


def _member_bars(
    inputs: RealtimeScreeningInput,
    security_id: UUID,
    policy: BreakoutPolicy,
) -> tuple[Mapping[str, object], ...]:
    rows = sorted(
        (
            row
            for row in inputs.bars
            if str(row["security_id"]) == str(security_id)
            and row["interval"] == policy.interval
            and row["adjustment_basis"] == "RAW_UNADJUSTED"
            and row["session_date"] == inputs.session
            and row["is_final"]
            and row["bar_end"] <= inputs.cutoff
        ),
        key=lambda row: (row["bar_end"], str(row["bar_revision_id"])),
    )
    return tuple(rows[-(policy.lookback + 1):])


def _return(rows: tuple[Mapping[str, object], ...]) -> Decimal | None:
    if len(rows) < 2:
        return None
    first, last = Decimal(str(rows[0]["close"])), Decimal(str(rows[-1]["close"]))
    return None if first <= 0 else last / first - Decimal(1)


def _bar_available_at(row: Mapping[str, object]) -> datetime:
    # Stream bars carry no publication clocks, so the optional clocks may be
    # empty; max() of a single bare datetime would try to iterate it.
    return max((
        row["bar_end"],
        *(
            row[name]
            for name in ("provider_published_at", "replay_available_at")
            if row.get(name) is not None
        ),
    ))


def _bar_received_at(row: Mapping[str, object]) -> datetime:
    return max((
        _bar_available_at(row),
        *(
            row[name]
            for name in ("observed_at", "created_at")
            if row.get(name) is not None
        ),
    ))


def _context_evidence(
    *,
    role: str,
    source_kind: str,
    source_id: object,
    payload_sha256: object,
    available_at: datetime,
    received_at: datetime,
    details: Mapping[str, object] | None = None,
) -> Mapping[str, object]:
    available = _utc(available_at)
    received = _utc(received_at)
    if received < available:
        raise ValueError(
            "canonical Stock Alert evidence receipt precedes availability"
        )
    return {
        "role": role,
        "source_kind": source_kind,
        "source_id": str(source_id),
        "payload_sha256": str(payload_sha256),
        "available_at": available.isoformat(),
        "received_at": received.isoformat(),
        "details": {} if details is None else dict(details),
    }


def _bar_evidence(
    row: Mapping[str, object],
    *,
    role: str,
) -> Mapping[str, object]:
    return _context_evidence(
        role=role,
        source_kind="BAR_REVISION",
        source_id=row["bar_revision_id"],
        payload_sha256=row["payload_sha256"],
        available_at=_bar_available_at(row),
        received_at=_bar_received_at(row),
        details={
            "security_id": str(row["security_id"]),
            "ticker": str(row["ticker"]),
            "interval": str(row["interval"]),
            "bar_start": row["bar_start"].isoformat(),
            "bar_end": row["bar_end"].isoformat(),
        },
    )


def _reference_and_sector(
    inputs: RealtimeScreeningInput,
    member: Mapping[str, object],
    members_by_ticker: Mapping[str, Mapping[str, object]],
    bars: Mapping[str, tuple[Mapping[str, object], ...]],
    policy: BreakoutPolicy,
) -> tuple[
    Decimal | None,
    tuple[Mapping[str, object], ...],
    Mapping[str, object],
]:
    reference_id = member.get("security_revision_id")
    reference_sha256 = member.get("payload_sha256")
    observed_at = member.get("observed_at")
    if (
        reference_id is None
        or reference_sha256 is None
        or observed_at is None
    ):
        return None, (), {
            "status": "UNAVAILABLE",
            "reason": "DATED_SECURITY_REFERENCE_MISSING",
        }
    reference = _context_evidence(
        role="SECURITY_REFERENCE",
        source_kind="SECURITY_REVISION",
        source_id=reference_id,
        payload_sha256=reference_sha256,
        available_at=observed_at,
        received_at=observed_at,
        details={
            "security_id": str(member["security_id"]),
            "ticker": str(member["ticker"]),
            "security_type": member.get("security_type"),
            "sector": member.get("sector"),
            "sic_code": member.get("sic_code"),
        },
    )
    security_type = member.get("security_type")
    retained_sector = member.get("sector")
    if security_type == "CS":
        if retained_sector not in SECTOR_BENCHMARK_ETF:
            return None, (reference,), {
                "status": "UNAVAILABLE",
                "reason": "RETAINED_SECTOR_MAPPING_MISSING",
                "security_type": security_type,
                "sector": retained_sector,
            }
        candidate = sector_benchmark_ticker(str(retained_sector))
    elif security_type == "ETF":
        candidate = sector_benchmark_ticker(None)
    else:
        return None, (reference,), {
            "status": "UNAVAILABLE",
            "reason": "SECTOR_POLICY_NOT_APPLICABLE",
            "security_type": security_type,
            "sector": retained_sector,
        }
    benchmark_ticker = resolve_benchmark_ticker(
        str(member["ticker"]),
        candidate,
    )
    mapping = {
        "contract": "canonical-stock-sector-benchmark-v1",
        "mapping_basis": "RETAINED_SECTOR_EXISTING_BENCHMARK_HELPER",
        "security_revision_id": str(reference_id),
        "security_payload_sha256": str(reference_sha256),
        "security_type": security_type,
        "retained_sector": retained_sector,
        "benchmark_ticker": benchmark_ticker,
    }
    mapping_sha256 = canonical_sha256(mapping)
    mapping_evidence = _context_evidence(
        role="SECTOR_MAPPING",
        source_kind="DERIVED_REFERENCE",
        source_id=uuid5(
            NAMESPACE_URL,
            f"canonical-stock-sector-mapping:{mapping_sha256}",
        ),
        payload_sha256=mapping_sha256,
        available_at=observed_at,
        received_at=observed_at,
        details=mapping,
    )
    benchmark = members_by_ticker.get(benchmark_ticker)
    benchmark_reference = None
    if benchmark is not None:
        benchmark_reference_id = benchmark.get(
            "security_revision_id"
        )
        benchmark_reference_sha256 = benchmark.get(
            "payload_sha256"
        )
        benchmark_observed_at = benchmark.get("observed_at")
        if (
            benchmark_reference_id is not None
            and benchmark_reference_sha256 is not None
            and benchmark_observed_at is not None
        ):
            benchmark_reference = _context_evidence(
                role="SECTOR_BENCHMARK_REFERENCE",
                source_kind="SECURITY_REVISION",
                source_id=benchmark_reference_id,
                payload_sha256=benchmark_reference_sha256,
                available_at=benchmark_observed_at,
                received_at=benchmark_observed_at,
                details={
                    "security_id": str(benchmark["security_id"]),
                    "ticker": str(benchmark["ticker"]),
                    "security_type": benchmark.get("security_type"),
                },
            )
    benchmark_rows = (
        ()
        if benchmark is None or benchmark_reference is None
        else bars[str(benchmark["security_id"])]
    )
    evidence = (
        reference,
        mapping_evidence,
        *((benchmark_reference,) if benchmark_reference is not None else ()),
        *(
            _bar_evidence(row, role="SECTOR_BAR")
            for row in benchmark_rows
        ),
    )
    trend = (
        _return(benchmark_rows)
        if len(benchmark_rows) == policy.lookback + 1
        else None
    )
    return trend, evidence, {
        "status": "READY" if trend is not None else "UNAVAILABLE",
        "reason": (
            None
            if trend is not None
            else (
                "CONTEXT_BENCHMARK_REFERENCE_MISSING"
                if benchmark_reference is None
                else "EXACT_SECTOR_BENCHMARK_WINDOW_MISSING"
            )
        ),
        "security_type": security_type,
        "sector": retained_sector,
        "benchmark_ticker": benchmark_ticker,
        "mapping_sha256": mapping_sha256,
    }


def _event_context(
    inputs: RealtimeScreeningInput,
    member: Mapping[str, object],
    market_events: tuple[Mapping[str, object], ...],
    market_event_coverage: tuple[Mapping[str, object], ...],
) -> tuple[
    bool | None,
    tuple[Mapping[str, object], ...],
    Mapping[str, object],
]:
    security_id = str(member["security_id"])
    security_type = member.get("security_type")
    window_start = inputs.cutoff - CANONICAL_STOCK_EVENT_LOOKBACK
    window_end = inputs.cutoff + CANONICAL_STOCK_EVENT_LOOKAHEAD
    if security_type == "CS":
        applicable_types = CANONICAL_STOCK_EVENT_TYPES
    elif security_type == "ETF":
        applicable_types = ("FED_RATE_DECISION",)
    else:
        applicable_types = ()

    def applies(row: Mapping[str, object]) -> bool:
        event_type = row["event_type"]
        row_security_id = row.get("security_id")
        return (
            event_type in applicable_types
            and (
                str(row_security_id) == security_id
                if event_type == "EARNINGS"
                else row_security_id is None
            )
        )

    active_events = tuple(sorted(
        (
            row
            for row in market_events
            if applies(row)
            and row["observed_at"] <= inputs.cutoff
            and row["received_at"] <= inputs.cutoff
            and row["valid_until"] > inputs.cutoff
            and window_start.date()
            <= row["event_date"]
            <= window_end.date()
            and row["status"] != "CANCELED"
        ),
        key=lambda row: (
            str(row["event_type"]),
            str(row["source"]),
            str(row["event_key"]),
            str(row["revision_id"]),
        ),
    ))
    eligible_coverage = tuple(sorted(
        (
            row
            for row in market_event_coverage
            if applies(row)
            and row["checked_at"] <= inputs.cutoff
            and row["valid_until"] > inputs.cutoff
            and row["window_start"] <= window_start.date()
            and row["window_end"] >= window_end.date()
        ),
        key=lambda row: (
            str(row["event_type"]),
            str(row["source"]),
            str(row["coverage_id"]),
        ),
    ))
    coverage_by_type = {
        event_type: tuple(
            row
            for row in eligible_coverage
            if row["event_type"] == event_type
        )
        for event_type in applicable_types
    }
    known_types = {
        event_type
        for event_type, rows in coverage_by_type.items()
        if any(
            row["verdict"] in {"COVERED", "KNOWN_NO_EVENT"}
            for row in rows
        )
    }
    event_risk = (
        None
        if not applicable_types
        else True
        if active_events
        else False
        if known_types == set(applicable_types)
        else None
    )
    event_evidence = tuple(
        _context_evidence(
            role="MARKET_EVENT",
            source_kind="MARKET_EVENT_REVISION",
            source_id=row["revision_id"],
            payload_sha256=row["payload_sha256"],
            available_at=row["observed_at"],
            received_at=row["received_at"],
            details={
                "security_id": (
                    None
                    if row.get("security_id") is None
                    else str(row["security_id"])
                ),
                "event_type": row["event_type"],
                "event_key": row["event_key"],
                "event_date": row["event_date"].isoformat(),
                "status": row["status"],
                "source": row["source"],
            },
        )
        for row in active_events
    )
    coverage_evidence = tuple(
        _context_evidence(
            role="MARKET_EVENT_COVERAGE",
            source_kind="MARKET_EVENT_COVERAGE",
            source_id=row["coverage_id"],
            payload_sha256=row["evidence_sha256"],
            available_at=row["checked_at"],
            received_at=row["checked_at"],
            details={
                "security_id": (
                    None
                    if row.get("security_id") is None
                    else str(row["security_id"])
                ),
                "event_type": row["event_type"],
                "window_start": row["window_start"].isoformat(),
                "window_end": row["window_end"].isoformat(),
                "source": row["source"],
                "verdict": row["verdict"],
            },
        )
        for row in eligible_coverage
    )
    return event_risk, (*event_evidence, *coverage_evidence), {
        "status": (
            "BLOCKED"
            if event_risk is True
            else "CLEAR"
            if event_risk is False
            else "UNAVAILABLE"
        ),
        "reason": (
            "RELEVANT_RETAINED_EVENT"
            if event_risk is True
            else None
            if event_risk is False
            else "EVENT_COVERAGE_UNKNOWN"
        ),
        "security_type": security_type,
        "applicable_event_types": applicable_types,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "known_event_types": tuple(sorted(known_types)),
    }


def _build_context(
    *,
    inputs: RealtimeScreeningInput,
    membership: CanonicalStockAlertMembership,
    identity: str,
    market_trend: Decimal | None,
    sector_trend: Decimal | None,
    event_risk: bool | None,
    evidence: tuple[Mapping[str, object], ...],
    reason_codes: tuple[str, ...],
    sector_policy: Mapping[str, object],
    event_policy: Mapping[str, object],
) -> CanonicalStockAlertContext:
    document = {
        "contract": CANONICAL_STOCK_CONTEXT_CONTRACT,
        "security_id": identity,
        "cohort_id": str(inputs.cohort_id),
        "desired_revision_id": (
            None
            if membership.desired_revision_id is None
            else str(membership.desired_revision_id)
        ),
        "desired_snapshot_sha256": membership.desired_snapshot_sha256,
        "membership_sha256": membership.membership_sha256,
        "cutoff": inputs.cutoff.isoformat(),
        "market_trend": (
            None if market_trend is None else canonical_value(market_trend)
        ),
        "sector_trend": (
            None if sector_trend is None else canonical_value(sector_trend)
        ),
        "event_risk": event_risk,
        "sector_policy": sector_policy,
        "event_policy": event_policy,
        "evidence": evidence,
        "reason_codes": reason_codes,
    }
    source_sha256 = canonical_sha256(document)
    source_record_id = uuid5(
        NAMESPACE_URL,
        f"canonical-equity-context:{source_sha256}",
    )
    available_at = (
        inputs.cutoff
        if not evidence
        else max(
            datetime.fromisoformat(str(row["available_at"]))
            for row in evidence
        )
    )
    received_at = (
        inputs.recorded_by
        if not evidence
        else max(
            datetime.fromisoformat(str(row["received_at"]))
            for row in evidence
        )
    )
    return CanonicalStockAlertContext(
        market_trend=market_trend,
        sector_trend=sector_trend,
        event_risk=event_risk,
        source_record_id=source_record_id,
        source_sha256=source_sha256,
        available_at=available_at,
        received_at=received_at,
        evidence=evidence,
        document=document,
        reason_codes=reason_codes,
    )


def canonical_stock_alert_contexts(
    inputs: RealtimeScreeningInput,
    policy: BreakoutPolicy = CANONICAL_STOCK_ALERT_POLICY,
    membership: CanonicalStockAlertMembership | None = None,
    *,
    context_members: tuple[Mapping[str, object], ...] = (),
    market_events: tuple[Mapping[str, object], ...] = (),
    market_event_coverage: tuple[Mapping[str, object], ...] = (),
) -> Mapping[str, CanonicalStockAlertContext]:
    """Freeze exact retained market, sector, and event context for each member."""
    membership = membership or canonical_stock_alert_membership(inputs)
    product_members = _product_members(inputs, membership)
    members = {
        str(row["security_id"]): row for row in product_members
    }
    context_by_identity = {
        str(row["security_id"]): row for row in context_members
    }
    if len(context_by_identity) != len(context_members):
        raise ValueError(
            "canonical Stock Alert context member identities are ambiguous"
        )
    all_members = dict(members)
    for identity, member in context_by_identity.items():
        existing = all_members.get(identity)
        if (
            existing is not None
            and str(existing["ticker"]) != str(member["ticker"])
        ):
            raise ValueError(
                "canonical Stock Alert context member identity changed"
            )
        all_members.setdefault(identity, member)
    members_by_ticker = {
        str(row["ticker"]): row for row in all_members.values()
    }
    if len(members_by_ticker) != len(all_members):
        raise ValueError(
            "canonical Stock Alert context member tickers are ambiguous"
        )
    bars = {
        identity: _member_bars(inputs, UUID(identity), policy)
        for identity in all_members
    }
    spy = next(
        (
            identity
            for identity, member in all_members.items()
            if member["ticker"] == "SPY"
        ),
        None,
    )
    market_rows = () if spy is None else bars[spy]
    market_trend = (
        _return(market_rows)
        if len(market_rows) == policy.lookback + 1
        else None
    )
    market_evidence = tuple(
        _bar_evidence(row, role="MARKET_BAR")
        for row in market_rows
    )
    result = {}
    for identity, member in members.items():
        sector_trend, sector_evidence, sector_policy = (
            _reference_and_sector(
                inputs,
                member,
                members_by_ticker,
                bars,
                policy,
            )
        )
        event_risk, event_evidence, event_policy = _event_context(
            inputs,
            member,
            market_events,
            market_event_coverage,
        )
        reasons = tuple(
            reason
            for value, reason in (
                (market_trend, "MISSING_MARKET_CONTEXT"),
                (sector_trend, "MISSING_SECTOR_CONTEXT"),
                (event_risk, "MISSING_EVENT_CONTEXT"),
            )
            if value is None
        )
        result[identity] = _build_context(
            inputs=inputs,
            membership=membership,
            identity=identity,
            market_trend=market_trend,
            sector_trend=sector_trend,
            event_risk=event_risk,
            evidence=(
                *market_evidence,
                *sector_evidence,
                *event_evidence,
            ),
            reason_codes=reasons,
            sector_policy=sector_policy,
            event_policy=event_policy,
        )
    return result


def _adjustment(inputs: RealtimeScreeningInput, security_id: UUID):
    evidence = {
        "actions": tuple(
            (str(row.get("action_id")), row.get("payload_sha256"))
            for row in inputs.actions
            if str(row["security_id"]) == str(security_id)
        ),
        "coverage": tuple(
            (str(row.get("coverage_id")), row.get("payload_sha256"))
            for row in inputs.coverage
            if str(row["security_id"]) == str(security_id)
        ),
    }
    return {
        "basis": "RAW",
        "action_sha256": canonical_sha256(evidence),
        "version": "realtime-v3-action-coverage-v1",
    }


def _prior_payload(value: Mapping[str, object] | PriorState | None):
    if value is None:
        return None
    state = value if isinstance(value, PriorState) else value.get("state", value)
    if isinstance(state, PriorState):
        state = canonical_value(state)
    return dict(state)


def _wire_input(
    inputs: RealtimeScreeningInput,
    member: Mapping[str, object],
    context: CanonicalStockAlertContext,
    policy: BreakoutPolicy,
    prior_state: Mapping[str, object] | PriorState | None,
) -> Mapping[str, object]:
    adjustment = _adjustment(inputs, UUID(str(member["security_id"])))
    bars = _member_bars(inputs, UUID(str(member["security_id"])), policy)
    wire_bars = []
    for row in bars:
        available_at = _bar_available_at(row)
        received_at = _bar_received_at(row)
        wire_bars.append({
            "source": {
                "security_id": str(member["security_id"]),
                "source_id": str(row["bar_revision_id"]),
                "revision": str(row["bar_revision_id"]),
                "payload_sha256": row["payload_sha256"],
            },
            "clocks": {
                "market_start": row["bar_start"].isoformat(),
                "market_end": row["bar_end"].isoformat(),
                "available_at": available_at.isoformat(),
                "received_at": received_at.isoformat(),
            },
            "adjustment": adjustment,
            "interval": row["interval"],
            "open": _decimal(row["open"]),
            "high": _decimal(row["high"]),
            "low": _decimal(row["low"]),
            "close": _decimal(row["close"]),
            "volume": _decimal(row["volume"]),
            "completed": True,
        })
    context_source = {
        "security_id": str(member["security_id"]),
        "source_id": str(context.source_record_id),
        "revision": "1",
        "payload_sha256": context.source_sha256,
    }
    if canonical_sha256(context.document) != context.source_sha256:
        raise ValueError(
            "canonical Stock Alert context evidence checksum changed"
        )
    return {
        "security": {
            "security_id": str(member["security_id"]),
            "ticker": member["ticker"],
            "exchange": member.get("exchange") or "XNYS",
            "currency": "USD",
        },
        "adjustment": adjustment,
        "model": canonical_value(policy.identity),
        "bars": wire_bars,
        "events": [],
        "context": {
            "source": context_source,
            "clocks": {
                "market_start": inputs.cutoff.isoformat(),
                "market_end": inputs.cutoff.isoformat(),
                "available_at": context.available_at.isoformat(),
                "received_at": context.received_at.isoformat(),
            },
            "market_trend": None if context.market_trend is None else str(context.market_trend),
            "sector_trend": None if context.sector_trend is None else str(context.sector_trend),
            "event_risk": context.event_risk,
            "evidence": list(context.evidence),
            "document": context.document,
            "reason_codes": list(context.reason_codes),
        },
        "market_cutoff": inputs.cutoff.isoformat(),
        "decision_at": inputs.recorded_by.isoformat(),
        "prior_state": _prior_payload(prior_state),
    }


def _event_inputs(packet, wire) -> tuple[RealtimeDetectorInput, ...]:
    values = []
    for bar, retained in zip(packet.bars, wire["bars"], strict=True):
        values.append(RealtimeDetectorInput(
            input_role="COMPLETED_BAR",
            input_key=bar.source.source_id,
            source_kind="BAR_REVISION",
            input_sha256=bar.source.payload_sha256,
            available_at=bar.clocks.available_at,
            received_at=bar.clocks.received_at,
            retained_input=retained,
            bar_revision_id=UUID(bar.source.source_id),
        ))
    context = packet.context
    values.append(RealtimeDetectorInput(
        input_role="EQUITY_CONTEXT",
        input_key=context.source.source_id,
        source_kind="REFERENCE",
        input_sha256=context.source.payload_sha256,
        available_at=context.clocks.available_at,
        received_at=context.clocks.received_at,
        retained_input=wire["context"],
        source_record_id=UUID(context.source.source_id),
    ))
    return tuple(values)


def _alert_projection(
    event: RealtimeDetectorEvent,
    member,
    decision: Decision,
    *,
    deliverable: bool,
):
    if decision.disposition is Disposition.NO_MATCH:
        return None
    matched = (
        decision.disposition is Disposition.MATCH
        and decision.geometry is not None
    )
    geometry = decision.geometry if matched else None
    trigger = None if geometry is None else float(geometry.trigger)
    stop = None if geometry is None else float(geometry.stop)
    target = None if geometry is None else float(geometry.target)
    risk = (
        None
        if trigger is None or stop is None
        else abs(trigger - stop)
    )
    projection = {
        "contract": CANONICAL_STOCK_ALERT_SCHEMA,
        "security_id": str(member["security_id"]),
        "ticker": member["ticker"],
        "company_name": member.get("company_name"),
        "model": CANONICAL_STOCK_ALERT_PRODUCT,
        "interval": "5m",
        "trade_style": "INTRADAY",
        "direction": (
            0 if decision.direction is None else int(decision.direction)
        ),
        "triggered_at": event.trigger_at.isoformat(),
        "published_at": event.decision_at.isoformat(),
        "trigger_price": trigger,
        "stop": stop,
        "target": target,
        "risk_pct": (
            None
            if trigger is None or trigger <= 0 or risk is None
            else risk / trigger
        ),
        "reward_risk": (
            None
            if risk is None or risk <= 0 or target is None or trigger is None
            else abs(target - trigger) / risk
        ),
        "status": "OPEN" if matched else "UNAVAILABLE",
        "lifecycle": "OPEN" if matched else "UNAVAILABLE",
        "lane": "TRADE" if matched else "OBSERVATION",
        "policy_version": decision.model.version,
        "policy_sha256": decision.model.policy_sha256,
        "feature_sha256": decision.features.feature_sha256,
        "decision_sha256": decision.decision_sha256,
        "state_key": decision.state.state_key,
        "state_revision": decision.state.revision,
        "reason_codes": list(decision.reason_codes),
        "gates": canonical_value(decision.gates),
        "reason": (
            None
            if matched
            else ", ".join(decision.reason_codes)
        ),
        "indicators": {},
        "indicator_status": (
            "AVAILABLE" if matched else "UNAVAILABLE"
        ),
        "context": {
            "status": "AVAILABLE" if matched else "UNAVAILABLE",
            "reason_codes": list(decision.reason_codes),
        },
        "warnings": (
            []
            if matched
            else [
                "Canonical model context unavailable; no actionable alert"
            ]
        ),
        "non_actionable": not matched,
        "deliverable": deliverable and matched,
        "delivery_permission": False,
        "publication_permission": False,
        "execution_permission": False,
    }
    return build_alert_projection(
        event,
        alert_kind="STOCK",
        lifecycle="OPEN" if matched else "UNAVAILABLE",
        projection_schema=CANONICAL_STOCK_ALERT_SCHEMA,
        projection=projection,
        destinations=("PORTAL",),
    )


def produce_canonical_stock_alert_batch(
    inputs: RealtimeScreeningInput,
    *,
    window_id: UUID,
    input_receipt_id: UUID,
    receipt_input_sha256: str,
    prior_states: Mapping[str, Mapping[str, object] | PriorState] | None = None,
    contexts: Mapping[str, CanonicalStockAlertContext] | None = None,
    membership: CanonicalStockAlertMembership | None = None,
    deadline: datetime | None = None,
    policy: BreakoutPolicy = CANONICAL_STOCK_ALERT_POLICY,
) -> CanonicalStockAlertBatch:
    prior_states = prior_states or {}
    membership = membership or canonical_stock_alert_membership(inputs)
    contexts = contexts or canonical_stock_alert_contexts(
        inputs,
        policy,
        membership,
    )
    normalized_deadline = None if deadline is None else _utc(deadline)
    is_late = (
        normalized_deadline is not None
        and inputs.recorded_by > normalized_deadline
    )
    version = canonical_stock_alert_version(policy)
    evaluations = []
    for member in _product_members(inputs, membership):
        identity = str(member["security_id"])
        wire = _wire_input(
            inputs,
            member,
            contexts[identity],
            policy,
            prior_states.get(identity),
        )
        packet = decode_input(wire)
        decision = evaluate(packet, policy)
        adapter = StableDetectorAdapterResult(
            adapter_id=CANONICAL_STOCK_ALERT_PRODUCT,
            cohort_id=inputs.cohort_id,
            provider_tier=inputs.provider_tier,
            input_sha256=packet.input_sha256,
            status=(
                "UNAVAILABLE"
                if decision.disposition in {Disposition.UNAVAILABLE, Disposition.ERROR}
                else "EXECUTED"
            ),
            output=decision,
            reason_codes=decision.reason_codes,
            publication_permission=False,
            execution_permission=False,
        )
        latest = packet.bars[-1] if packet.bars else None
        trigger_at = inputs.cutoff if latest is None else latest.clocks.market_end
        trigger_identity = decision.trigger_identity or model_sha256({
            "kind": "canonical-equity-evaluation",
            "state_key": decision.state.state_key,
            "observation_sha256": packet.observation_sha256,
        })
        payload = {
            "contract": CANONICAL_STOCK_EVALUATION_SCHEMA,
            "product": CANONICAL_STOCK_ALERT_PRODUCT,
            "window_id": str(window_id),
            "input_receipt_id": str(input_receipt_id),
            "receipt_input_sha256": receipt_input_sha256,
            "screening_input_sha256": inputs_sha256(inputs),
            "desired_revision_id": (
                None
                if membership.desired_revision_id is None
                else str(membership.desired_revision_id)
            ),
            "desired_snapshot_sha256": (
                membership.desired_snapshot_sha256
            ),
            "membership_sha256": membership.membership_sha256,
            "security_id": identity,
            "ticker": member["ticker"],
            "context_source_record_id": str(
                contexts[identity].source_record_id
            ),
            "context_source_sha256": contexts[identity].source_sha256,
            "decision": canonical_value(decision),
            "context_reason_codes": list(contexts[identity].reason_codes),
            "observation_session": inputs.session.isoformat(),
            "observation_cutoff": inputs.cutoff.isoformat(),
            "completed_at": inputs.recorded_by.isoformat(),
            "late_catch_up": is_late,
            "deliverable": not is_late,
            "publication_permission": False,
            "execution_permission": False,
        }
        event = build_detector_event(
            adapter,
            version,
            instrument_id=int(member["instrument_id"]),
            trigger_identity=trigger_identity,
            trigger_at=trigger_at,
            decision_at=inputs.recorded_by,
            valid_until=decision.valid_until,
            disposition=decision.disposition.value,
            direction=None if decision.direction is None else int(decision.direction),
            reason_codes=decision.reason_codes,
            readiness_epoch=1,
            payload_schema=CANONICAL_STOCK_EVALUATION_SCHEMA,
            payload=payload,
            inputs=_event_inputs(packet, wire),
        )
        state = None
        if decision.state.revision > 0:
            state_payload = {
                "security_id": identity,
                "ticker": member["ticker"],
                "state": canonical_value(decision.state),
            }
            state = RealtimeDetectorState(
                detector_sha256=version.detector_sha256,
                cohort_id=inputs.cohort_id,
                state_key=decision.state.state_key,
                revision=decision.state.revision,
                state_schema=CANONICAL_STOCK_STATE_SCHEMA,
                state_sha256=canonical_sha256(state_payload),
                state_payload=state_payload,
                updated_at=inputs.cutoff,
            )
        alert = _alert_projection(
            event,
            member,
            decision,
            deliverable=not is_late,
        )
        evaluations.append(CanonicalStockAlertEvaluation(
            security_id=UUID(identity),
            instrument_id=int(member["instrument_id"]),
            ticker=member["ticker"],
            input_sha256=packet.input_sha256,
            decision=decision,
            event=event,
            state=state,
            alert=alert,
        ))
    return CanonicalStockAlertBatch(
        cohort_id=inputs.cohort_id,
        window_id=window_id,
        input_receipt_id=input_receipt_id,
        session_date=inputs.session,
        cutoff=inputs.cutoff,
        receipt_input_sha256=receipt_input_sha256,
        detector_input_sha256=inputs_sha256(inputs),
        membership=membership,
        version=version,
        evaluations=tuple(evaluations),
        completed_at=inputs.recorded_by,
        deadline=normalized_deadline,
        is_late=is_late,
    )
