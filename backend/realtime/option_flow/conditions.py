"""Pinned, reviewed interpretation of provider option trade conditions (semantics v2).

The provider payload (GET /v3/reference/conditions, asset_class=options, data_type=trade) is
read from a file. A reviewed interpretation is bound to the canonical sha256 of that payload,
so a different payload cannot be accepted silently. A code absent from the reviewed table
fails closed: the print is not volume eligible, not classified, and carries a reason code.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from ..contracts import canonical_sha256

SEMANTICS_VERSION = "massive_options_conditions_v2"
PROVENANCE_LIVE_CAPTURE = "LIVE_CAPTURED"
PROVENANCE_SYNTHETIC = "SYNTHETIC_SCHEMA_FIXTURE_NOT_LIVE_CAPTURED"
PROVENANCES = frozenset({PROVENANCE_LIVE_CAPTURE, PROVENANCE_SYNTHETIC})
PROVIDER_LIVE = "massive"
PROVIDER_SYNTHETIC = "massive_synthetic_fixture"
ASSET_CLASS = "options"
DATA_TYPE = "trade"
_VERDICT_CACHE_LIMIT = 4096


class ConditionPinError(ValueError):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


class Treatment(str, Enum):
    INCLUDED = "INCLUDED"
    EXCLUDED = "EXCLUDED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class ProviderCondition:
    """One condition exactly as the provider described it."""

    condition_id: int
    abbreviation: str | None
    name: str
    description: str | None
    condition_type: str
    legacy: bool
    exchange: int | None
    data_types: tuple[str, ...]
    update_rules: Mapping[str, Any]
    sip_mapping: Mapping[str, Any]

    @property
    def consolidated_updates_volume(self) -> bool | None:
        consolidated = self.update_rules.get("consolidated")
        if not isinstance(consolidated, Mapping):
            return None
        value = consolidated.get("updates_volume")
        return value if isinstance(value, bool) else None


@dataclass(frozen=True, slots=True)
class ReviewedCondition:
    """Our reviewed interpretation of one provider code."""

    condition_id: int
    treatment: Treatment
    is_late: bool = False
    is_multi_leg: bool = False
    is_cross: bool = False
    is_sweep: bool = False
    is_floor: bool = False
    review_note: str | None = None

    @property
    def volume_eligible(self) -> bool:
        return self.treatment is Treatment.INCLUDED

    @property
    def is_cancel(self) -> bool:
        return self.treatment is Treatment.CANCELED


@dataclass(frozen=True, slots=True)
class ConditionVerdict:
    treatment: Treatment
    volume_eligible: bool
    is_cancel: bool
    is_late: bool
    is_multi_leg: bool
    is_cross: bool
    is_sweep: bool
    is_floor: bool
    reasons: tuple[str, ...]

    @property
    def known(self) -> bool:
        return self.treatment is not Treatment.UNKNOWN


@dataclass(frozen=True, slots=True)
class SemanticsPin:
    semantics_version: str
    payload_sha256: str
    provenance: str


@dataclass(frozen=True, slots=True)
class TradeConditionRow:
    provider: str
    asset_class: str
    data_type: str
    condition_id: int
    abbreviation: str | None
    name: str
    description: str | None
    condition_type: str
    legacy: bool
    exchange: int | None
    update_rules: Mapping[str, Any]
    sip_mapping: Mapping[str, Any]
    fetched_at: datetime
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class SemanticsRow:
    semantics_version: str
    condition_id: int
    treatment: str
    volume_eligible: bool
    is_cancel: bool
    is_late: bool
    is_multi_leg: bool
    is_cross: bool
    is_sweep: bool
    is_floor: bool
    source_payload_sha256: str
    reviewed_at: datetime
    review_note: str | None


@dataclass(frozen=True, slots=True)
class ConditionPayload:
    provenance: str
    fetched_at: datetime
    results: tuple[Mapping[str, Any], ...]
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class ConditionReview:
    semantics_version: str
    provenance: str
    reviewed_payload_sha256: str
    reviewed_at: datetime
    codes: tuple[ReviewedCondition, ...]


@dataclass(frozen=True, slots=True)
class ConditionSemantics:
    """The pinned semantics version object consumed by the engine."""

    pin: SemanticsPin
    semantics_sha256: str
    fetched_at: datetime
    reviewed_at: datetime
    provider: tuple[ProviderCondition, ...]
    reviewed: Mapping[int, ReviewedCondition]
    unreviewed_ids: tuple[int, ...]
    _verdicts: dict = field(default_factory=dict, repr=False, compare=False)

    @property
    def version(self) -> str:
        return self.pin.semantics_version

    @property
    def is_live_capture(self) -> bool:
        return self.pin.provenance == PROVENANCE_LIVE_CAPTURE

    def classify(self, conditions: tuple[int, ...]) -> ConditionVerdict:
        verdict = self._verdicts.get(conditions)
        if verdict is None:
            verdict = self._derive(conditions)
            if len(self._verdicts) >= _VERDICT_CACHE_LIMIT:
                self._verdicts.clear()
            self._verdicts[conditions] = verdict
        return verdict

    def _derive(self, conditions: tuple[int, ...]) -> ConditionVerdict:
        if not conditions:
            return _unknown(("TRADE_CONDITION_MISSING",))
        rows: list[ReviewedCondition] = []
        unknown: set[int] = set()
        for code in conditions:
            row = self.reviewed.get(code)
            if row is None:
                unknown.add(code)
            else:
                rows.append(row)
        if unknown:
            return _unknown(tuple(f"UNKNOWN_TRADE_CONDITION_{code}" for code in sorted(unknown)))
        if any(row.is_cancel for row in rows):
            treatment, reasons = Treatment.CANCELED, ("PROVIDER_CANCELED_CONDITION",)
        elif any(row.treatment is Treatment.EXCLUDED for row in rows):
            treatment, reasons = Treatment.EXCLUDED, ("PROVIDER_NON_VOLUME_CONDITION",)
        else:
            treatment, reasons = Treatment.INCLUDED, ("PROVIDER_CONSOLIDATED_VOLUME_ELIGIBLE",)
        return ConditionVerdict(
            treatment=treatment,
            volume_eligible=treatment is Treatment.INCLUDED,
            is_cancel=treatment is Treatment.CANCELED,
            is_late=any(row.is_late for row in rows),
            is_multi_leg=any(row.is_multi_leg for row in rows),
            is_cross=any(row.is_cross for row in rows),
            is_sweep=any(row.is_sweep for row in rows),
            is_floor=any(row.is_floor for row in rows),
            reasons=reasons,
        )

    def trade_condition_rows(self, provider: str | None = None) -> tuple[TradeConditionRow, ...]:
        if provider is None:
            provider = PROVIDER_LIVE if self.is_live_capture else PROVIDER_SYNTHETIC
        return tuple(
            TradeConditionRow(
                provider=provider,
                asset_class=ASSET_CLASS,
                data_type=DATA_TYPE,
                condition_id=row.condition_id,
                abbreviation=row.abbreviation,
                name=row.name,
                description=row.description,
                condition_type=row.condition_type,
                legacy=row.legacy,
                exchange=row.exchange,
                update_rules=dict(row.update_rules),
                sip_mapping=dict(row.sip_mapping),
                fetched_at=self.fetched_at,
                payload_sha256=self.pin.payload_sha256,
            )
            for row in self.provider
            if DATA_TYPE in row.data_types
        )

    def semantics_rows(self) -> tuple[SemanticsRow, ...]:
        return tuple(
            SemanticsRow(
                semantics_version=self.pin.semantics_version,
                condition_id=row.condition_id,
                treatment=row.treatment.value,
                volume_eligible=row.volume_eligible,
                is_cancel=row.is_cancel,
                is_late=row.is_late,
                is_multi_leg=row.is_multi_leg,
                is_cross=row.is_cross,
                is_sweep=row.is_sweep,
                is_floor=row.is_floor,
                source_payload_sha256=self.pin.payload_sha256,
                reviewed_at=self.reviewed_at,
                review_note=row.review_note,
            )
            for _, row in sorted(self.reviewed.items())
        )


def _unknown(reasons: tuple[str, ...]) -> ConditionVerdict:
    return ConditionVerdict(
        treatment=Treatment.UNKNOWN,
        volume_eligible=False,
        is_cancel=False,
        is_late=False,
        is_multi_leg=False,
        is_cross=False,
        is_sweep=False,
        is_floor=False,
        reasons=reasons,
    )


def _timestamp(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise ConditionPinError("TIMESTAMP_INVALID", name)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ConditionPinError("TIMESTAMP_INVALID", name) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ConditionPinError("TIMESTAMP_INVALID", f"{name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def payload_sha256(results: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]]) -> str:
    """Content address of the provider facts: request scope plus results sorted by id.

    The response envelope (request_id, next_url, counts) is transient and excluded.
    """
    try:
        ordered = sorted(results, key=lambda row: row["id"])
    except (KeyError, TypeError) as error:
        raise ConditionPinError("PAYLOAD_FIELD_MISSING", "id") from error
    return canonical_sha256({"asset_class": ASSET_CLASS, "data_type": DATA_TYPE, "results": ordered})


def parse_payload(document: Mapping[str, Any]) -> ConditionPayload:
    provenance = document.get("provenance")
    if provenance not in PROVENANCES:
        raise ConditionPinError("PROVENANCE_UNRECOGNIZED", str(provenance))
    request = document.get("request")
    if not isinstance(request, Mapping) or (
        request.get("asset_class") != ASSET_CLASS or request.get("data_type") != DATA_TYPE
    ):
        raise ConditionPinError("PAYLOAD_SCOPE_MISMATCH", "expected options trade conditions")
    results = document.get("results")
    if not isinstance(results, list) or not results:
        raise ConditionPinError("PAYLOAD_RESULTS_MISSING")
    fetched_at = _timestamp(document.get("fetched_at"), "fetched_at")
    return ConditionPayload(
        provenance=provenance,
        fetched_at=fetched_at,
        results=tuple(results),
        payload_sha256=payload_sha256(results),
    )


def _provider_condition(raw: Mapping[str, Any]) -> ProviderCondition:
    for required in ("id", "name", "type"):
        if required not in raw:
            raise ConditionPinError("PAYLOAD_FIELD_MISSING", required)
    condition_id, name, condition_type = raw["id"], raw["name"], raw["type"]
    if type(condition_id) is not int or condition_id < 0:
        raise ConditionPinError("PAYLOAD_FIELD_INVALID", "id")
    if not isinstance(name, str) or not name.strip() or not isinstance(condition_type, str) or not condition_type.strip():
        raise ConditionPinError("PAYLOAD_FIELD_INVALID", f"name/type of {condition_id}")
    asset_class = raw.get("asset_class", ASSET_CLASS)
    if asset_class != ASSET_CLASS:
        raise ConditionPinError("PAYLOAD_SCOPE_MISMATCH", f"asset_class of {condition_id}")
    update_rules = raw.get("update_rules", {})
    sip_mapping = raw.get("sip_mapping", {})
    data_types = raw.get("data_types", [])
    if not isinstance(update_rules, Mapping) or not isinstance(sip_mapping, Mapping):
        raise ConditionPinError("PAYLOAD_FIELD_INVALID", f"rules of {condition_id}")
    if not isinstance(data_types, list) or any(not isinstance(item, str) for item in data_types):
        raise ConditionPinError("PAYLOAD_FIELD_INVALID", f"data_types of {condition_id}")
    exchange = raw.get("exchange")
    if exchange is not None and (type(exchange) is not int or exchange < 0):
        raise ConditionPinError("PAYLOAD_FIELD_INVALID", f"exchange of {condition_id}")
    return ProviderCondition(
        condition_id=condition_id,
        abbreviation=raw.get("abbreviation"),
        name=name,
        description=raw.get("description"),
        condition_type=condition_type,
        legacy=bool(raw.get("legacy", False)),
        exchange=exchange,
        data_types=tuple(data_types),
        update_rules=update_rules,
        sip_mapping=sip_mapping,
    )


def parse_review(document: Mapping[str, Any]) -> ConditionReview:
    version = document.get("semantics_version")
    provenance = document.get("provenance")
    pinned = document.get("reviewed_payload_sha256")
    if not isinstance(version, str) or not version.strip():
        raise ConditionPinError("REVIEW_FIELD_INVALID", "semantics_version")
    if provenance not in PROVENANCES:
        raise ConditionPinError("PROVENANCE_UNRECOGNIZED", str(provenance))
    if not isinstance(pinned, str) or len(pinned) != 64 or pinned != pinned.lower() or any(
        character not in "0123456789abcdef" for character in pinned
    ):
        raise ConditionPinError("REVIEW_FIELD_INVALID", "reviewed_payload_sha256")
    codes = document.get("codes")
    if not isinstance(codes, list) or not codes:
        raise ConditionPinError("REVIEW_CODES_MISSING")
    reviewed: list[ReviewedCondition] = []
    for raw in codes:
        try:
            treatment = Treatment(raw["treatment"])
            condition_id = raw["condition_id"]
        except (KeyError, ValueError, TypeError) as error:
            raise ConditionPinError("REVIEW_FIELD_INVALID", "condition_id/treatment") from error
        if treatment is Treatment.UNKNOWN or type(condition_id) is not int or condition_id < 0:
            raise ConditionPinError("REVIEW_FIELD_INVALID", f"condition {condition_id}")
        flags = {name: raw.get(name, False) for name in
                 ("is_late", "is_multi_leg", "is_cross", "is_sweep", "is_floor")}
        if any(type(value) is not bool for value in flags.values()):
            raise ConditionPinError("REVIEW_FIELD_INVALID", f"flags of {condition_id}")
        reviewed.append(ReviewedCondition(condition_id, treatment, review_note=raw.get("review_note"), **flags))
    return ConditionReview(
        semantics_version=version,
        provenance=provenance,
        reviewed_payload_sha256=pinned,
        reviewed_at=_timestamp(document.get("reviewed_at"), "reviewed_at"),
        codes=tuple(reviewed),
    )


def build_semantics(
    payload: ConditionPayload,
    review: ConditionReview,
    *,
    expected_payload_sha256: str | None = None,
) -> ConditionSemantics:
    """Bind a review to its payload. Any mismatch raises ConditionPinError."""
    pinned = review.reviewed_payload_sha256 if expected_payload_sha256 is None else expected_payload_sha256
    if payload.payload_sha256 != pinned or review.reviewed_payload_sha256 != pinned:
        raise ConditionPinError(
            "PAYLOAD_HASH_MISMATCH",
            f"payload {payload.payload_sha256[:12]} review {review.reviewed_payload_sha256[:12]} "
            f"pin {pinned[:12]}",
        )
    if payload.provenance != review.provenance:
        raise ConditionPinError("PROVENANCE_MISMATCH", f"{payload.provenance} vs {review.provenance}")
    provider = tuple(sorted((_provider_condition(row) for row in payload.results),
                            key=lambda row: row.condition_id))
    by_id = {row.condition_id: row for row in provider}
    if len(by_id) != len(provider):
        raise ConditionPinError("PAYLOAD_DUPLICATE_ID")
    reviewed: dict[int, ReviewedCondition] = {}
    for row in review.codes:
        if row.condition_id in reviewed:
            raise ConditionPinError("REVIEW_DUPLICATE_ID", str(row.condition_id))
        source = by_id.get(row.condition_id)
        if source is None:
            raise ConditionPinError("REVIEW_CODE_NOT_IN_PAYLOAD", str(row.condition_id))
        if DATA_TYPE not in source.data_types:
            raise ConditionPinError("REVIEW_CODE_NOT_A_TRADE_CONDITION", str(row.condition_id))
        if row.volume_eligible and source.consolidated_updates_volume is not True:
            raise ConditionPinError("PROVIDER_VOLUME_RULE_CONFLICT", str(row.condition_id))
        reviewed[row.condition_id] = row
    unreviewed = tuple(sorted(set(by_id) - set(reviewed)))
    semantics_sha256 = canonical_sha256({
        "semantics_version": review.semantics_version,
        "payload_sha256": payload.payload_sha256,
        "provenance": review.provenance,
        "codes": [reviewed[key] for key in sorted(reviewed)],
    })
    return ConditionSemantics(
        pin=SemanticsPin(review.semantics_version, payload.payload_sha256, review.provenance),
        semantics_sha256=semantics_sha256,
        fetched_at=payload.fetched_at,
        reviewed_at=review.reviewed_at,
        provider=provider,
        reviewed=reviewed,
        unreviewed_ids=unreviewed,
    )


def load_semantics(
    payload_path: Path,
    review_path: Path,
    *,
    expected_payload_sha256: str | None = None,
) -> ConditionSemantics:
    """Read a payload and its review from files and return the pinned semantics."""
    payload = parse_payload(json.loads(Path(payload_path).read_text(encoding="utf-8")))
    review = parse_review(json.loads(Path(review_path).read_text(encoding="utf-8")))
    return build_semantics(payload, review, expected_payload_sha256=expected_payload_sha256)
