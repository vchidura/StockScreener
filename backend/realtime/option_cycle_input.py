"""The V2 input of an options cycle: ids and hashes, never bytes (v3 review findings 1 and 6; design doc, open item 11).

The v1 forward-input encoder (`forward_storage.encode_forward_inputs`, un-ported lineage, not extended here)
serializes every item's raw payload and underlying-bar tuple twice plus base64 discovery bytes, so its size grows
with contracts and bars and reaches its 64 MiB cap at 17 contracts per underlying for 25 underlyings at 390 bars
per item. An options cycle in V2 does not carry evidence, it points at it:

- one fixed-size entry per option underlying: the discovery batch id, the batch evidence digest, the page payload
  chain digest, the coverage digest, the request filter digest, counts and the included expiration and strike
  windows (all of which are stored in `rt.option_discovery_batches` and `rt.option_discovery_coverage`),
- the cycle header: lineage identity, provider tier, scheduled cycle, capability profile and universe digests,
  policy, configuration and code digests, the cohort and slot-set identity (`member_sha256`), and the underlying
  set identity (`count` and `sha256`, the shape `lineage_head.reference_set` uses).

There is no field that can hold response bytes, base64 pages or a per-contract tuple, so the encoded size is a
function of the number of underlyings only (contract counts change a few digits of two integers per entry).
`MAX_OPTION_CYCLE_INPUT_BYTES` is a hard bound enforced on encode and, before any parsing, on decode.

`verify_option_cycle_input_against_tables` is the loader port: it re-resolves every referenced batch through
`option_discovery_verify` (read-only) and the slot set through `option_slot_set_repository`, and fails closed on
any digest, count, window or identity that differs from what is stored.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

from .contracts import canonical_json
from .lineage_head import rows_identity
from .option_discovery_repository import RealtimeOptionDiscoveryEvidence
from .option_discovery_verify import coverage_sha256, payload_chain_sha256, read_discovery_batches
from .option_slot_set_repository import load_slot_set_version_cursor
from .options_evidence_identity import (
    ADVANCED_REALTIME_V2_LINEAGE, V2_OPTIONS_EVIDENCE_IDENTITY, OptionsEvidenceIdentity,
    is_v2_options_evidence_identity,
)
from .preflight import ProviderTier

OPTION_CYCLE_INPUT_VERSION = "option_cycle_input_v1"
MAX_OPTION_CYCLE_UNDERLYINGS = 50
MAX_OPTION_CYCLE_INPUT_BYTES = 131_072
_MAXIMUM_PAGES = 40
_MAXIMUM_ROWS = 10_000
_MAXIMUM_RESPONSE_BYTES = 67_108_864
_SHA256 = re.compile(r"[0-9a-f]{64}")
_DECIMAL_STEP = Decimal("0.00000001")
_MAXIMUM_SCALED = Decimal(10) ** 12


def _digest(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _bounded_int(value: object, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def _uuid(value: object, name: str) -> UUID:
    if type(value) is not UUID:
        raise ValueError(f"{name} must be a UUID")
    return value


def _scaled(value: object, name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite() or not 0 < value < _MAXIMUM_SCALED:
        raise ValueError(f"{name} must be a positive finite Decimal below 10^12")
    scaled = value.quantize(_DECIMAL_STEP)
    if scaled != value:
        raise ValueError(f"{name} has more than eight decimal places")
    return scaled


def _clock(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _fixed(value: Decimal) -> str:
    return format(value, "f")


@dataclass(frozen=True, slots=True)
class UnderlyingSetIdentity:
    count: int
    sha256: str


def underlying_set_identity(members: Sequence[tuple[str, UUID]]) -> UnderlyingSetIdentity:
    """Count and digest of the (underlyer, security id) set, independent of the order it is given in."""
    ordered = sorted((symbol, str(security_id)) for symbol, security_id in members)
    identity = rows_identity([dict(underlyer=symbol, underlying_security_id=security_id) for symbol, security_id in ordered])
    return UnderlyingSetIdentity(identity["count"], identity["sha256"])


@dataclass(frozen=True, slots=True)
class OptionCycleUnderlying:
    """One underlying's reference to its stored discovery batch. Fixed size: every field is an id, a digest,
    a count or a window boundary."""

    underlyer: str
    underlying_security_id: UUID
    discovery_batch_id: UUID
    discovery_evidence_sha256: str
    payload_chain_sha256: str
    coverage_sha256: str
    request_filter_sha256: str
    page_count: int
    row_count: int
    response_bytes: int
    expiration_from: date
    expiration_through: date
    strike_min: Decimal
    strike_max: Decimal

    def __post_init__(self) -> None:
        if (type(self.underlyer) is not str or not self.underlyer or len(self.underlyer) > 32
                or self.underlyer != self.underlyer.strip().upper()):
            raise ValueError("cycle underlyer must be a non-blank upper-case symbol of at most 32 characters")
        _uuid(self.underlying_security_id, "cycle underlying security id")
        _uuid(self.discovery_batch_id, "cycle discovery batch id")
        for name in ("discovery_evidence_sha256", "payload_chain_sha256", "coverage_sha256", "request_filter_sha256"):
            _digest(getattr(self, name), name)
        _bounded_int(self.page_count, "cycle page count", 1, _MAXIMUM_PAGES)
        _bounded_int(self.row_count, "cycle row count", 0, _MAXIMUM_ROWS)
        _bounded_int(self.response_bytes, "cycle response bytes", 1, _MAXIMUM_RESPONSE_BYTES)
        if (type(self.expiration_from) is not date or type(self.expiration_through) is not date
                or self.expiration_through < self.expiration_from):
            raise ValueError("cycle expiration window is invalid")
        object.__setattr__(self, "strike_min", _scaled(self.strike_min, "cycle strike minimum"))
        object.__setattr__(self, "strike_max", _scaled(self.strike_max, "cycle strike maximum"))
        if self.strike_max < self.strike_min:
            raise ValueError("cycle strike window is invalid")


@dataclass(frozen=True, slots=True)
class OptionCycleInput:
    provider_tier: ProviderTier
    scheduled_cycle: datetime
    profile_id: UUID
    profile_sha256: str
    universe_sha256: str
    policy_sha256: str
    configuration_sha256: str
    code_sha256: str
    cohort_id: UUID
    underlyings: tuple[OptionCycleUnderlying, ...]
    slot_set_version: int | None = None
    slot_set_member_sha256: str | None = None
    identity: OptionsEvidenceIdentity = V2_OPTIONS_EVIDENCE_IDENTITY
    underlying_set: UnderlyingSetIdentity = field(init=False)

    def __post_init__(self) -> None:
        if not is_v2_options_evidence_identity(self.identity):
            raise ValueError("an options cycle input must carry the advanced_realtime_v2 evidence identity")
        if type(self.provider_tier) is not ProviderTier:
            raise ValueError("cycle provider tier is invalid")
        object.__setattr__(self, "scheduled_cycle", _clock(self.scheduled_cycle, "cycle scheduled_cycle"))
        _uuid(self.profile_id, "cycle profile id")
        _uuid(self.cohort_id, "cycle cohort id")
        for name in ("profile_sha256", "universe_sha256", "policy_sha256", "configuration_sha256", "code_sha256"):
            _digest(getattr(self, name), name)
        if (self.slot_set_version is None) != (self.slot_set_member_sha256 is None):
            raise ValueError("cycle slot set version and member hash are given together or not at all")
        if self.slot_set_version is not None:
            _bounded_int(self.slot_set_version, "cycle slot set version", 1, 2_147_483_647)
            _digest(self.slot_set_member_sha256, "cycle slot set member hash")
        entries = tuple(self.underlyings)
        if (not 1 <= len(entries) <= MAX_OPTION_CYCLE_UNDERLYINGS
                or any(type(entry) is not OptionCycleUnderlying for entry in entries)):
            raise ValueError(f"an options cycle input has 1 to {MAX_OPTION_CYCLE_UNDERLYINGS} typed underlyings")
        ordered = tuple(sorted(entries, key=lambda entry: entry.underlyer))
        if (len({entry.underlyer for entry in ordered}) != len(ordered)
                or len({entry.discovery_batch_id for entry in ordered}) != len(ordered)
                or len({entry.underlying_security_id for entry in ordered}) != len(ordered)):
            raise ValueError("cycle underlyings and their discovery batches must be unique")
        object.__setattr__(self, "underlyings", ordered)
        object.__setattr__(self, "underlying_set", underlying_set_identity(
            [(entry.underlyer, entry.underlying_security_id) for entry in ordered]))

    @property
    def sha256(self) -> str:
        return encode_option_cycle_input(self)[1]


def _entry_payload(entry: OptionCycleUnderlying) -> dict[str, object]:
    return dict(
        underlyer=entry.underlyer, underlying_security_id=str(entry.underlying_security_id),
        discovery_batch_id=str(entry.discovery_batch_id), discovery_evidence_sha256=entry.discovery_evidence_sha256,
        payload_chain_sha256=entry.payload_chain_sha256, coverage_sha256=entry.coverage_sha256,
        request_filter_sha256=entry.request_filter_sha256, page_count=entry.page_count, row_count=entry.row_count,
        response_bytes=entry.response_bytes, expiration_from=entry.expiration_from.isoformat(),
        expiration_through=entry.expiration_through.isoformat(), strike_min=_fixed(entry.strike_min),
        strike_max=_fixed(entry.strike_max),
    )


def _payload(cycle_input: OptionCycleInput) -> dict[str, object]:
    return dict(
        version=OPTION_CYCLE_INPUT_VERSION, lineage=cycle_input.identity.lineage,
        provider_tier=cycle_input.provider_tier.value, scheduled_cycle=_iso(cycle_input.scheduled_cycle),
        profile_id=str(cycle_input.profile_id), profile_sha256=cycle_input.profile_sha256,
        universe_sha256=cycle_input.universe_sha256, policy_sha256=cycle_input.policy_sha256,
        configuration_sha256=cycle_input.configuration_sha256, code_sha256=cycle_input.code_sha256,
        cohort_id=str(cycle_input.cohort_id), slot_set_version=cycle_input.slot_set_version,
        slot_set_member_sha256=cycle_input.slot_set_member_sha256,
        underlying_set=dict(count=cycle_input.underlying_set.count, sha256=cycle_input.underlying_set.sha256),
        underlyings=[_entry_payload(entry) for entry in cycle_input.underlyings],
    )


def encode_option_cycle_input(cycle_input: OptionCycleInput) -> tuple[str, str]:
    """Canonical JSON text and its SHA-256 (the input's identity). Over the byte bound is an error."""
    if type(cycle_input) is not OptionCycleInput:
        raise TypeError("encode requires an OptionCycleInput")
    text = canonical_json(_payload(cycle_input))
    encoded = text.encode("ascii")
    if len(encoded) > MAX_OPTION_CYCLE_INPUT_BYTES:
        raise ValueError(f"option cycle input is {len(encoded)} bytes; the bound is {MAX_OPTION_CYCLE_INPUT_BYTES}")
    return text, hashlib.sha256(encoded).hexdigest()


def _unique_pairs(pairs):
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError("option cycle input has a duplicate key")
    return dict(pairs)


def _reject_constant(value):
    raise ValueError("option cycle input has a non-integer or non-finite number")


def _exact(mapping: object, keys: set[str], name: str) -> dict:
    if type(mapping) is not dict or set(mapping) != keys:
        raise ValueError(f"option cycle input {name} has unexpected or missing fields")
    return mapping


_ENTRY_KEYS = {
    "underlyer", "underlying_security_id", "discovery_batch_id", "discovery_evidence_sha256", "payload_chain_sha256",
    "coverage_sha256", "request_filter_sha256", "page_count", "row_count", "response_bytes", "expiration_from",
    "expiration_through", "strike_min", "strike_max",
}
_TOP_KEYS = {
    "version", "lineage", "provider_tier", "scheduled_cycle", "profile_id", "profile_sha256", "universe_sha256",
    "policy_sha256", "configuration_sha256", "code_sha256", "cohort_id", "slot_set_version",
    "slot_set_member_sha256", "underlying_set", "underlyings",
}


def _text(value: object, name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"option cycle input {name} must be text")
    return value


def _entry_from_payload(raw: object) -> OptionCycleUnderlying:
    item = _exact(raw, _ENTRY_KEYS, "underlying entry")
    return OptionCycleUnderlying(
        underlyer=item["underlyer"], underlying_security_id=UUID(_text(item["underlying_security_id"], "security id")),
        discovery_batch_id=UUID(_text(item["discovery_batch_id"], "batch id")),
        discovery_evidence_sha256=item["discovery_evidence_sha256"], payload_chain_sha256=item["payload_chain_sha256"],
        coverage_sha256=item["coverage_sha256"], request_filter_sha256=item["request_filter_sha256"],
        page_count=item["page_count"], row_count=item["row_count"], response_bytes=item["response_bytes"],
        expiration_from=date.fromisoformat(_text(item["expiration_from"], "expiration_from")),
        expiration_through=date.fromisoformat(_text(item["expiration_through"], "expiration_through")),
        strike_min=Decimal(_text(item["strike_min"], "strike_min")),
        strike_max=Decimal(_text(item["strike_max"], "strike_max")),
    )


def decode_option_cycle_input(payload_text: str, payload_sha256: str) -> OptionCycleInput:
    """Verify and decode. The size bound is checked before parsing, the digest before trusting any field, every
    level must have exactly its fields, and the decoded input must re-encode to the identical canonical text."""
    if type(payload_text) is not str:
        raise TypeError("option cycle input payload must be text")
    try:
        encoded = payload_text.encode("ascii")
    except UnicodeEncodeError as error:
        raise ValueError("option cycle input is canonical ASCII JSON") from error
    if len(encoded) > MAX_OPTION_CYCLE_INPUT_BYTES:
        raise ValueError(f"option cycle input exceeds the {MAX_OPTION_CYCLE_INPUT_BYTES} byte bound")
    if _digest(payload_sha256, "option cycle input digest") != hashlib.sha256(encoded).hexdigest():
        raise ValueError("option cycle input digest mismatch")
    try:
        payload = _exact(json.loads(payload_text, object_pairs_hook=_unique_pairs, parse_float=_reject_constant,
            parse_constant=_reject_constant), _TOP_KEYS, "payload")
        if payload["version"] != OPTION_CYCLE_INPUT_VERSION or payload["lineage"] != ADVANCED_REALTIME_V2_LINEAGE:
            raise ValueError("option cycle input version or lineage is not supported")
        members = _exact(payload["underlying_set"], {"count", "sha256"}, "underlying_set")
        if type(payload["underlyings"]) is not list:
            raise ValueError("option cycle input underlyings must be a list")
        cycle_input = OptionCycleInput(
            provider_tier=ProviderTier(payload["provider_tier"]),
            scheduled_cycle=datetime.fromisoformat(_text(payload["scheduled_cycle"], "scheduled_cycle")),
            profile_id=UUID(_text(payload["profile_id"], "profile_id")), profile_sha256=payload["profile_sha256"],
            universe_sha256=payload["universe_sha256"], policy_sha256=payload["policy_sha256"],
            configuration_sha256=payload["configuration_sha256"], code_sha256=payload["code_sha256"],
            cohort_id=UUID(_text(payload["cohort_id"], "cohort_id")),
            underlyings=tuple(_entry_from_payload(row) for row in payload["underlyings"]),
            slot_set_version=payload["slot_set_version"], slot_set_member_sha256=payload["slot_set_member_sha256"],
        )
    except (KeyError, TypeError, ArithmeticError) as error:
        raise ValueError("option cycle input payload is malformed") from error
    if (members["count"] != cycle_input.underlying_set.count or members["sha256"] != cycle_input.underlying_set.sha256
            or encode_option_cycle_input(cycle_input)[0] != payload_text):
        raise ValueError("option cycle input underlying set or canonical form does not round-trip")
    return cycle_input


def option_cycle_underlying_from_evidence(evidence: RealtimeOptionDiscoveryEvidence) -> OptionCycleUnderlying:
    if type(evidence) is not RealtimeOptionDiscoveryEvidence:
        raise TypeError("a cycle entry is built from complete discovery evidence")
    batch = evidence.result.batch
    return OptionCycleUnderlying(
        underlyer=batch.underlyer, underlying_security_id=evidence.underlying_security_id,
        discovery_batch_id=batch.batch_id, discovery_evidence_sha256=evidence.evidence_sha256,
        payload_chain_sha256=payload_chain_sha256((page.page_number, page.payload_sha256) for page in batch.pages),
        coverage_sha256=coverage_sha256(evidence.coverage), request_filter_sha256=batch.request_filter_sha256,
        page_count=len(batch.pages), row_count=batch.row_count, response_bytes=evidence.response_bytes,
        expiration_from=evidence.expiration_from, expiration_through=evidence.expiration_through,
        strike_min=evidence.strike_min, strike_max=evidence.strike_max,
    )


def build_option_cycle_input(
    evidence: Sequence[RealtimeOptionDiscoveryEvidence], *, configuration_sha256: str, code_sha256: str,
    cohort_id: UUID, slot_set_version: int | None = None, slot_set_member_sha256: str | None = None,
) -> OptionCycleInput:
    """The cycle input for one cycle's discovery evidence (one batch per underlying, all under one capability
    profile, scheduled cycle and market policy)."""
    rows = tuple(evidence)
    if not rows:
        raise ValueError("an options cycle input needs discovery evidence")
    entries = tuple(option_cycle_underlying_from_evidence(row) for row in rows)
    first, batch = rows[0], rows[0].result.batch
    if any(row.scope != first.scope or row.result.batch.scheduled_cycle != batch.scheduled_cycle
            or row.result.batch.policy_sha256 != batch.policy_sha256 for row in rows):
        raise ValueError("cycle discovery evidence must share one profile, tier, scheduled cycle and policy")
    return OptionCycleInput(
        provider_tier=first.scope.provider_tier, scheduled_cycle=batch.scheduled_cycle,
        profile_id=first.scope.profile_id, profile_sha256=first.scope.profile_sha256,
        universe_sha256=first.scope.universe.sha256, policy_sha256=batch.policy_sha256,
        configuration_sha256=configuration_sha256, code_sha256=code_sha256, cohort_id=cohort_id, underlyings=entries,
        slot_set_version=slot_set_version, slot_set_member_sha256=slot_set_member_sha256,
    )


@dataclass(frozen=True, slots=True)
class OptionCycleInputVerification:
    batches_verified: int
    batches_with_pages: int
    batches_pages_pruned: int
    slot_set_verified: bool


def _differences(pairs) -> list[str]:
    return [name for name, expected, stored in pairs if expected != stored]


def verify_option_cycle_input_against_tables(
    cursor, cycle_input: OptionCycleInput, *, require_pages: bool = False,
) -> OptionCycleInputVerification:
    """Re-resolve every referenced discovery batch (and the slot set) and fail closed on any difference.

    Batch and coverage rows are permanent evidence. Page rows follow the 14-day HOT_TTL window: when a batch
    has none left it counts as pruned, unless `require_pages`; a partly retained page set is always an error.
    Page bodies are not read: the stored payload hashes were verified against them when the batch was persisted."""
    if type(cycle_input) is not OptionCycleInput:
        raise TypeError("verification requires an OptionCycleInput")
    stored_batches = read_discovery_batches(cursor, [entry.discovery_batch_id for entry in cycle_input.underlyings])
    with_pages = pruned = 0
    for entry in cycle_input.underlyings:
        stored = stored_batches.get(entry.discovery_batch_id)
        if stored is None:
            raise ValueError(f"cycle discovery batch for {entry.underlyer} is not stored")
        different = _differences((
            ("provider_tier", cycle_input.provider_tier.value, stored.provider_tier),
            ("profile_id", cycle_input.profile_id, stored.profile_id),
            ("profile_sha256", cycle_input.profile_sha256, stored.profile_sha256),
            ("universe_sha256", cycle_input.universe_sha256, stored.universe_sha256),
            ("policy_sha256", cycle_input.policy_sha256, stored.policy_sha256),
            ("scheduled_cycle", cycle_input.scheduled_cycle,
                stored.scheduled_cycle if stored.scheduled_cycle.tzinfo is not None else None),
            ("underlyer", entry.underlyer, stored.underlyer),
            ("underlying_security_id", entry.underlying_security_id, stored.underlying_security_id),
            ("evidence_sha256", entry.discovery_evidence_sha256, stored.evidence_sha256),
            ("request_filter_sha256", entry.request_filter_sha256, stored.request_filter_sha256),
            ("page_count", entry.page_count, stored.page_count),
            ("row_count", entry.row_count, stored.row_count),
            ("response_bytes", entry.response_bytes, stored.response_bytes),
            ("expiration_from", entry.expiration_from, stored.expiration_from),
            ("expiration_through", entry.expiration_through, stored.expiration_through),
            ("strike_min", entry.strike_min, stored.strike_min),
            ("strike_max", entry.strike_max, stored.strike_max),
        ))
        if different:
            raise ValueError(f"stored discovery batch for {entry.underlyer} differs from the cycle input: "
                + ", ".join(different))
        if len(stored.coverage) != 8 or stored.coverage_digest != entry.coverage_sha256:
            raise ValueError(f"stored discovery coverage for {entry.underlyer} differs from the cycle input")
        if not stored.pages:
            if require_pages:
                raise ValueError(f"discovery pages for {entry.underlyer} are required but no longer stored")
            pruned += 1
            continue
        if (tuple(page.page_number for page in stored.pages) != tuple(range(1, entry.page_count + 1))
                or sum(page.response_bytes for page in stored.pages) != entry.response_bytes
                or stored.payload_chain_digest != entry.payload_chain_sha256):
            raise ValueError(f"stored discovery pages for {entry.underlyer} differ from the cycle input")
        with_pages += 1
    slot_set_verified = False
    if cycle_input.slot_set_version is not None:
        slot_set = load_slot_set_version_cursor(
            cursor, cycle_input.cohort_id, cycle_input.slot_set_version, verify_identities=False)
        if slot_set is None:
            raise ValueError("cycle slot set version is not stored")
        if slot_set.member_sha256 != cycle_input.slot_set_member_sha256:
            raise ValueError("stored slot set members differ from the cycle input")
        slot_set_verified = True
    return OptionCycleInputVerification(len(cycle_input.underlyings), with_pages, pruned, slot_set_verified)


def load_verified_option_cycle_input(
    cursor, payload_text: str, payload_sha256: str, *, require_pages: bool = False,
) -> tuple[OptionCycleInput, OptionCycleInputVerification]:
    """Decode a stored cycle input and verify it against the tables in one step."""
    cycle_input = decode_option_cycle_input(payload_text, payload_sha256)
    return cycle_input, verify_option_cycle_input_against_tables(cursor, cycle_input, require_pages=require_pages)
