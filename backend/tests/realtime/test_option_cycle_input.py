"""The V2 options-cycle input: ids and hashes, never bytes. Offline: the 'tables' are a recording fake cursor."""
import hashlib
import json
from dataclasses import dataclass, fields, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from random import Random
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

import realtime.option_cycle_input as module
from realtime.contracts import MarketChannel, canonical_sha256
from realtime.option_cycle_input import (
    MAX_OPTION_CYCLE_INPUT_BYTES, MAX_OPTION_CYCLE_UNDERLYINGS, OPTION_CYCLE_INPUT_VERSION, OptionCycleInput,
    OptionCycleUnderlying, build_option_cycle_input, decode_option_cycle_input, encode_option_cycle_input,
    load_verified_option_cycle_input, option_cycle_underlying_from_evidence, underlying_set_identity,
    verify_option_cycle_input_against_tables,
)
from realtime.option_discovery_repository import OptionDiscoveryCoverageBoundary
from realtime.option_discovery_verify import coverage_sha256, payload_chain_sha256
from realtime.option_slot_set import SlotSetKind, SlotSetMember, SlotSetPurpose, SlotSetVersion
from realtime.options_evidence_identity import OptionsEvidenceIdentity, V2_OPTIONS_EVIDENCE_IDENTITY
from realtime.preflight import ProviderTier
from test_forward_ingestion import cycle  # noqa: F401 - pytest fixture
from test_option_flow_realtime import flow_matrices

CYCLE_AT = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc)
PROFILE_ID = uuid5(NAMESPACE_URL, "oid-profile")
COHORT_ID = uuid5(NAMESPACE_URL, "oid-cohort")


def sha(*parts) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()


PROFILE_SHA, UNIVERSE_SHA, POLICY_SHA, CONFIGURATION_SHA, CODE_SHA = (sha(name) for name in
    ("profile", "universe", "policy", "configuration", "code"))
BATCH_COLUMNS = ("discovery_batch_id", "underlying_security_id", "profile_id", "profile_sha256", "universe_sha256",
                 "provider_tier", "underlyer", "scheduled_cycle", "expiration_from", "expiration_through", "strike_min",
                 "strike_max", "request_filter_sha256", "policy_sha256", "page_count", "row_count", "response_bytes",
                 "evidence_sha256")
BOUNDARIES = (("EXPIRATION_DATE", "INCLUDED", "GTE", "2026-10-07", None),
              ("EXPIRATION_DATE", "INCLUDED", "LTE", "2026-12-04", None),
              ("EXPIRATION_DATE", "EXCLUDED", "LT", "2026-10-07", "OUTSIDE_DISCOVERY_WINDOW"),
              ("EXPIRATION_DATE", "EXCLUDED", "GT", "2026-12-04", "OUTSIDE_DISCOVERY_WINDOW"),
              ("STRIKE_PRICE", "INCLUDED", "GTE", "85.00", None),
              ("STRIKE_PRICE", "INCLUDED", "LTE", "115.00", None),
              ("STRIKE_PRICE", "EXCLUDED", "LT", "85.00", "OUTSIDE_STRIKE_CORRIDOR"),
              ("STRIKE_PRICE", "EXCLUDED", "GT", "115.00", "OUTSIDE_STRIKE_CORRIDOR"))


@dataclass
class Stored:
    batch: dict
    coverage: list
    pages: list
    entry: OptionCycleUnderlying


def stored_batch(index, *, pages=2, rows=12, symbol=None):
    symbol = symbol or f"SYM{index:02d}"
    batch_id, security_id = uuid5(NAMESPACE_URL, f"oid-batch:{symbol}"), uuid5(NAMESPACE_URL, f"oid-security:{symbol}")
    page_rows = [(number, sha(symbol, "page", number), 1_000 * number) for number in range(1, pages + 1)]
    coverage = [OptionDiscoveryCoverageBoundary(*row) for row in BOUNDARIES]
    total = sum(size for _, _, size in page_rows)
    batch = dict(discovery_batch_id=str(batch_id), underlying_security_id=str(security_id), profile_id=str(PROFILE_ID),
        profile_sha256=PROFILE_SHA, universe_sha256=UNIVERSE_SHA, provider_tier="MASSIVE_DEVELOPER", underlyer=symbol,
        scheduled_cycle=CYCLE_AT, expiration_from=date(2026, 10, 7), expiration_through=date(2026, 12, 4),
        strike_min=Decimal("85.00000000"), strike_max=Decimal("115.00000000"),
        request_filter_sha256=sha(symbol, "filter"), policy_sha256=POLICY_SHA, page_count=pages, row_count=rows,
        response_bytes=total, evidence_sha256=sha(symbol, "evidence"))
    entry = OptionCycleUnderlying(symbol, security_id, batch_id, batch["evidence_sha256"],
        payload_chain_sha256((number, digest) for number, digest, _ in page_rows), coverage_sha256(coverage),
        batch["request_filter_sha256"], pages, rows, total, date(2026, 10, 7), date(2026, 12, 4),
        Decimal("85.00"), Decimal("115.00"))
    return Stored(batch, coverage, page_rows, entry)


def header(entries, **overrides):
    values = dict(provider_tier=ProviderTier.MASSIVE_DEVELOPER, scheduled_cycle=CYCLE_AT, profile_id=PROFILE_ID,
        profile_sha256=PROFILE_SHA, universe_sha256=UNIVERSE_SHA, policy_sha256=POLICY_SHA,
        configuration_sha256=CONFIGURATION_SHA, code_sha256=CODE_SHA, cohort_id=COHORT_ID, underlyings=tuple(entries))
    values.update(overrides)
    return OptionCycleInput(**values)


def slot_set(version=1):
    members = (SlotSetMember(1, MarketChannel.QUOTE, SlotSetPurpose.PRIMARY, 1, 1),
               SlotSetMember(2, MarketChannel.QUOTE, SlotSetPurpose.PRIMARY, 2, 2))
    return SlotSetVersion(COHORT_ID, version, SlotSetKind.PRE_SESSION if version == 1 else SlotSetKind.REFRESH,
                          CYCLE_AT, sha("slot-policy"), members)


class FakeTables:
    """Answers the verifier's SELECTs from in-memory rows and records every statement."""

    def __init__(self, stored, slots=None):
        self.stored = {item.batch["discovery_batch_id"]: item for item in stored}
        self.slots = slots
        self.statements = []
        self._rows = []

    def execute(self, sql, params=()):
        self.statements.append(sql)
        ids = set(params[0]) if params and isinstance(params[0], list) else set()
        chosen = {key: item for key, item in self.stored.items() if key in ids}
        if "FROM rt.option_discovery_batches" in sql:
            rows = [tuple(item.batch[column] for column in BATCH_COLUMNS) for item in chosen.values()]
        elif "FROM rt.option_discovery_coverage" in sql:
            rows = [(key, row.dimension, row.disposition, row.comparison, row.boundary, row.reason_code)
                    for key, item in sorted(chosen.items()) for row in item.coverage]
        elif "FROM rt.option_discovery_pages" in sql:
            rows = [(key, number, digest, size) for key, item in sorted(chosen.items())
                    for number, digest, size in sorted(item.pages)]
        elif "FROM rt.slot_set_versions" in sql:
            rows = [] if self.slots is None else [(self.slots.kind.value, self.slots.effective_at,
                self.slots.policy_sha256, self.slots.member_sha256, self.slots.primary_contract_count,
                self.slots.reserve_contract_count)]
        elif "FROM rt.slot_set_members" in sql:
            rows = [(row.instrument_id, row.channel.value, row.purpose.value, row.priority, row.catalog_version_id)
                    for row in (self.slots.members if self.slots else ())]
        else:
            raise AssertionError(f"unexpected SQL: {sql}")
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


def world(count=3, *, slots=None, **kwargs):
    stored = [stored_batch(index, **kwargs) for index in range(count)]
    extra = {} if slots is None else dict(slot_set_version=slots.version, slot_set_member_sha256=slots.member_sha256)
    return stored, header([item.entry for item in stored], **extra), FakeTables(stored, slots)


# --- shape and encoding ---------------------------------------------------------------------------------------

def test_an_entry_has_no_field_that_can_carry_response_bytes_pages_or_a_per_contract_tuple():
    assert [item.name for item in fields(OptionCycleUnderlying)] == [
        "underlyer", "underlying_security_id", "discovery_batch_id", "discovery_evidence_sha256",
        "payload_chain_sha256", "coverage_sha256", "request_filter_sha256", "page_count", "row_count",
        "response_bytes", "expiration_from", "expiration_through", "strike_min", "strike_max"]
    assert {item.type for item in fields(OptionCycleUnderlying)} == {"str", "UUID", "int", "date", "Decimal"}
    assert not any(item.type.startswith(("tuple", "list", "bytes", "dict")) for item in fields(OptionCycleInput)
                   if item.name != "underlyings")
    text, _ = encode_option_cycle_input(header([stored_batch(0).entry]))
    payload = json.loads(text)
    assert all(type(value) in (str, int, type(None), dict, list) for value in payload.values())
    assert all(type(value) in (str, int) for value in payload["underlyings"][0].values())


def test_the_encoding_is_canonical_deterministic_and_independent_of_input_order():
    entries = [stored_batch(index).entry for index in range(6)]
    shuffled = list(entries)
    Random(7).shuffle(shuffled)
    first, second = header(entries), header(shuffled)
    text, digest = encode_option_cycle_input(first)
    assert (text, digest) == encode_option_cycle_input(second) == encode_option_cycle_input(first)
    assert digest == hashlib.sha256(text.encode("ascii")).hexdigest() == first.sha256 == canonical_sha256(json.loads(text))
    assert text == json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"))
    payload = json.loads(text)
    assert payload["version"] == OPTION_CYCLE_INPUT_VERSION and payload["lineage"] == "advanced_realtime_v2"
    assert [row["underlyer"] for row in payload["underlyings"]] == sorted(row.underlyer for row in entries)
    assert payload["underlying_set"] == {"count": 6, "sha256": underlying_set_identity(
        [(row.underlyer, row.underlying_security_id) for row in reversed(entries)]).sha256}
    assert header(entries, configuration_sha256=sha("other")).sha256 != digest


def test_every_identity_input_changes_the_cycle_identity():
    entries = [stored_batch(index).entry for index in range(2)]
    base = header(entries).sha256
    slots = slot_set()
    changed = [header(entries, scheduled_cycle=CYCLE_AT.replace(minute=15)), header(entries, policy_sha256=sha("p2")),
        header(entries, code_sha256=sha("c2")), header(entries, cohort_id=uuid5(NAMESPACE_URL, "other")),
        header(entries, provider_tier=ProviderTier.MASSIVE_ADVANCED), header(entries[:1]),
        header(entries, slot_set_version=slots.version, slot_set_member_sha256=slots.member_sha256),
        header([replace(entries[0], row_count=13), entries[1]]),
        header([replace(entries[0], discovery_evidence_sha256=sha("x")), entries[1]])]
    assert len({row.sha256 for row in changed} | {base}) == len(changed) + 1


def test_round_trip_through_the_verifier_rebuilds_the_identical_input():
    stored, with_slots, _ = world(4, slots=slot_set())
    for original in (header([item.entry for item in stored]), with_slots):
        text, digest = encode_option_cycle_input(original)
        decoded = decode_option_cycle_input(text, digest)
        assert decoded == original and encode_option_cycle_input(decoded) == (text, digest)
        assert decoded.underlying_set == original.underlying_set and decoded.identity == V2_OPTIONS_EVIDENCE_IDENTITY


@pytest.mark.parametrize("mutate", [
    lambda payload: payload.update(extra=1),
    lambda payload: payload.pop("code_sha256"),
    lambda payload: payload["underlyings"][0].update(response_body="e30="),
    lambda payload: payload["underlyings"][0].pop("row_count"),
    lambda payload: payload["underlying_set"].update(extra=1),
    lambda payload: payload.update(version="option_cycle_input_v2"),
    lambda payload: payload.update(lineage="stocks_realtime_v1"),
    lambda payload: payload.update(lineage=""),
    lambda payload: payload.update(lineage="advanced_realtime_v2 "),
    lambda payload: payload.update(provider_tier="MASSIVE_DELAYED"),
    lambda payload: payload.update(scheduled_cycle="2026-10-07T15:00:00"),
    lambda payload: payload.update(cohort_id="not-a-uuid"),
    lambda payload: payload.update(code_sha256="A" * 64),
    lambda payload: payload.update(slot_set_version=1),
    lambda payload: payload["underlying_set"].update(count=99),
    lambda payload: payload["underlyings"][0].update(row_count=True),
    lambda payload: payload["underlyings"][0].update(row_count=1.5),
    lambda payload: payload["underlyings"][0].update(strike_min="85.000000001"),
    lambda payload: payload["underlyings"][0].update(strike_min="NaN"),
    lambda payload: payload["underlyings"][0].update(page_count=41),
    lambda payload: payload["underlyings"][0].update(underlyer="aapl"),
    lambda payload: payload.update(underlyings={}),
])
def test_decode_rejects_any_payload_that_is_not_exactly_the_contract_even_when_rehashed(mutate):
    text, _ = encode_option_cycle_input(header([stored_batch(index).entry for index in range(2)]))
    payload = json.loads(text)
    mutate(payload)
    tampered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    with pytest.raises(ValueError):
        decode_option_cycle_input(tampered, hashlib.sha256(tampered.encode()).hexdigest())


def test_decode_rejects_a_changed_digest_a_non_canonical_form_duplicate_keys_and_non_ascii():
    original = header([stored_batch(0).entry])
    text, digest = encode_option_cycle_input(original)
    with pytest.raises(ValueError, match="digest mismatch"):
        decode_option_cycle_input(text, sha("other"))
    with pytest.raises(ValueError, match="digest mismatch"):
        decode_option_cycle_input(text.replace("SYM00", "SYM01"), digest)
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        decode_option_cycle_input(text, digest.upper())
    spaced = json.dumps(json.loads(text), sort_keys=True)
    with pytest.raises(ValueError, match="round-trip"):
        decode_option_cycle_input(spaced, hashlib.sha256(spaced.encode()).hexdigest())
    duplicated = text.replace('"version"', '"version":"option_cycle_input_v1","version"', 1)
    with pytest.raises(ValueError, match="duplicate key"):
        decode_option_cycle_input(duplicated, hashlib.sha256(duplicated.encode()).hexdigest())
    for odd in ('{"version":"é"}', "not json", "[]", "null"):
        with pytest.raises(ValueError):
            decode_option_cycle_input(odd, hashlib.sha256(odd.encode()).hexdigest())
    with pytest.raises(TypeError):
        decode_option_cycle_input(text.encode(), digest)
    with pytest.raises(TypeError):
        encode_option_cycle_input({"version": OPTION_CYCLE_INPUT_VERSION})


@pytest.mark.parametrize(("change", "message"), [
    (dict(identity=OptionsEvidenceIdentity("")), "advanced_realtime_v2 evidence identity"),
    (dict(identity=OptionsEvidenceIdentity("advanced_realtime_v2 ")), "advanced_realtime_v2 evidence identity"),
    (dict(identity=OptionsEvidenceIdentity("stocks_realtime_v1")), "advanced_realtime_v2 evidence identity"),
    (dict(identity="advanced_realtime_v2"), "advanced_realtime_v2 evidence identity"),
    (dict(provider_tier="MASSIVE_DEVELOPER"), "provider tier"),
    (dict(scheduled_cycle=CYCLE_AT.replace(tzinfo=None)), "timezone-aware"),
    (dict(profile_sha256="0" * 63), "SHA-256"),
    (dict(code_sha256="G" * 64), "SHA-256"),
    (dict(cohort_id=str(COHORT_ID)), "UUID"),
    (dict(slot_set_version=1), "given together"),
    (dict(slot_set_member_sha256=sha("m")), "given together"),
    (dict(slot_set_version=0, slot_set_member_sha256=sha("m")), "slot set version"),
    (dict(slot_set_version=1, slot_set_member_sha256="x"), "SHA-256"),
    (dict(underlyings=()), "1 to 50"),
    (dict(underlyings=("AAPL",)), "1 to 50"),
])
def test_the_cycle_header_is_validated(change, message):
    with pytest.raises(ValueError, match=message):
        header([stored_batch(0).entry], **change)


def test_underlyings_are_unique_and_bounded():
    first = stored_batch(0).entry
    with pytest.raises(ValueError, match="unique"):
        header([first, first])
    with pytest.raises(ValueError, match="unique"):
        header([first, replace(stored_batch(1).entry, discovery_batch_id=first.discovery_batch_id)])
    with pytest.raises(ValueError, match="unique"):
        header([first, replace(stored_batch(1).entry, underlying_security_id=first.underlying_security_id)])
    assert len(header([stored_batch(index).entry for index in range(MAX_OPTION_CYCLE_UNDERLYINGS)]).underlyings) == 50
    with pytest.raises(ValueError, match="1 to 50"):
        header([stored_batch(index).entry for index in range(MAX_OPTION_CYCLE_UNDERLYINGS + 1)])


@pytest.mark.parametrize(("change", "message"), [
    (dict(underlyer="aapl"), "upper-case"), (dict(underlyer=""), "upper-case"), (dict(underlyer=" AAPL"), "upper-case"),
    (dict(underlyer="A" * 33), "upper-case"), (dict(underlying_security_id="x"), "UUID"),
    (dict(discovery_batch_id=None), "UUID"), (dict(discovery_evidence_sha256="a"), "SHA-256"),
    (dict(payload_chain_sha256="A" * 64), "SHA-256"), (dict(coverage_sha256=None), "SHA-256"),
    (dict(request_filter_sha256="0" * 65), "SHA-256"), (dict(page_count=0), "page count"),
    (dict(page_count=41), "page count"), (dict(row_count=-1), "row count"), (dict(row_count=10_001), "row count"),
    (dict(row_count=True), "row count"), (dict(response_bytes=0), "response bytes"),
    (dict(response_bytes=67_108_865), "response bytes"), (dict(expiration_through=date(2026, 10, 6)), "expiration window"),
    (dict(expiration_from="2026-10-07"), "expiration window"), (dict(strike_min=Decimal("0")), "positive"),
    (dict(strike_min=85.0), "positive"), (dict(strike_max=Decimal("NaN")), "positive"),
    (dict(strike_min=Decimal("85.123456789")), "eight decimal places"), (dict(strike_max=Decimal("84")), "strike window"),
    (dict(strike_max=Decimal("1" + "0" * 13)), "positive finite|eight decimal"),
])
def test_an_entry_is_validated_against_the_stored_table_bounds(change, message):
    with pytest.raises(ValueError, match=message):
        replace(stored_batch(0).entry, **change)


# --- the byte bound ------------------------------------------------------------------------------------------

def _wide(index, *, rows, pages=40, symbol=None):
    return replace(stored_batch(index, symbol=symbol).entry, page_count=pages, row_count=rows,
                   response_bytes=67_108_864)


def test_the_encoded_size_depends_on_the_underlying_count_not_on_contracts_pages_or_bytes():
    sizes = {}
    for count in (8, 25, 50):
        for rows in (1, 12, 16, 1_000, 10_000):
            entries = [_wide(index, rows=rows) for index in range(count)]
            sizes[count, rows] = len(encode_option_cycle_input(header(entries))[0].encode("ascii"))
    for count in (8, 25, 50):
        spread = max(sizes[count, rows] for rows in (1, 12, 16, 1_000, 10_000)) - min(
            sizes[count, rows] for rows in (1, 12, 16, 1_000, 10_000))
        assert spread <= count * 5  # only the digits of one integer per underlying change
    assert sizes[8, 12] < sizes[25, 12] < sizes[50, 12] < MAX_OPTION_CYCLE_INPUT_BYTES // 2
    per_underlying = (sizes[50, 10_000] - sizes[8, 10_000]) / 42
    assert 600 < per_underlying < 900
    assert sizes[50, 10_000] < MAX_OPTION_CYCLE_INPUT_BYTES // 2 and MAX_OPTION_CYCLE_INPUT_BYTES * 512 <= 64 * 1024 * 1024


def test_the_worst_case_input_with_widest_symbols_stays_inside_the_hard_bound():
    entries = [_wide(index, rows=10_000, symbol=f"{chr(65 + index // 26)}{chr(65 + index % 26)}".ljust(32, "X"))
               for index in range(MAX_OPTION_CYCLE_UNDERLYINGS)]
    slots = slot_set()
    text, digest = encode_option_cycle_input(header(entries, slot_set_version=slots.version,
        slot_set_member_sha256=slots.member_sha256, provider_tier=ProviderTier.MASSIVE_ADVANCED))
    assert len(text) < MAX_OPTION_CYCLE_INPUT_BYTES * 0.6
    assert decode_option_cycle_input(text, digest).underlyings[0].underlyer.endswith("X")


def test_the_bound_is_enforced_on_encode_and_before_any_parsing_on_decode(monkeypatch):
    original = header([stored_batch(index).entry for index in range(3)])
    text, digest = encode_option_cycle_input(original)
    monkeypatch.setattr(module, "MAX_OPTION_CYCLE_INPUT_BYTES", len(text) - 1)
    with pytest.raises(ValueError, match="the bound is"):
        encode_option_cycle_input(original)
    with pytest.raises(ValueError, match="byte bound"):
        decode_option_cycle_input(text, digest)
    monkeypatch.undo()
    oversized = "x" * (MAX_OPTION_CYCLE_INPUT_BYTES + 1)

    def forbidden(*args, **kwargs):
        raise AssertionError("an oversized payload must be rejected before parsing")

    monkeypatch.setattr(module.json, "loads", forbidden)
    with pytest.raises(ValueError, match="byte bound"):
        decode_option_cycle_input(oversized, hashlib.sha256(oversized.encode()).hexdigest())


def test_the_module_does_not_use_or_extend_the_v1_forward_input_encoder():
    with open(module.__file__, encoding="utf-8") as handle:
        source = handle.read()
    code = "\n".join(line for line in source.splitlines() if line.startswith(("import ", "from ")))
    assert "forward_" not in code and "encode_forward_inputs" not in code


# --- building from real discovery evidence --------------------------------------------------------------------

def test_the_input_is_built_from_complete_discovery_evidence_without_any_response_bytes(cycle):
    _, scope, _, evidence = flow_matrices(cycle)
    discoveries = [row.discovery for row in evidence]
    slots = slot_set()
    built = build_option_cycle_input(discoveries, configuration_sha256=CONFIGURATION_SHA, code_sha256=CODE_SHA,
        cohort_id=scope.cohort_id, slot_set_version=slots.version, slot_set_member_sha256=slots.member_sha256)
    assert [row.underlyer for row in built.underlyings] == sorted(scope.underlyings)
    assert built.provider_tier is ProviderTier.MASSIVE_DEVELOPER and built.scheduled_cycle == scope.cutoff
    assert built.profile_sha256 == scope.selection.scope.profile_sha256
    assert built.universe_sha256 == scope.selection.scope.universe.sha256
    by_symbol = {row.discovery.result.batch.underlyer: row.discovery for row in evidence}
    for entry in built.underlyings:
        source = by_symbol[entry.underlyer]
        batch = source.result.batch
        assert entry == option_cycle_underlying_from_evidence(source)
        assert (entry.discovery_batch_id, entry.discovery_evidence_sha256) == (batch.batch_id, source.evidence_sha256)
        assert entry.payload_chain_sha256 == payload_chain_sha256((page.page_number, page.payload_sha256) for page in batch.pages)
        assert entry.coverage_sha256 == coverage_sha256(source.coverage)
        assert (entry.page_count, entry.row_count, entry.response_bytes) == (
            len(batch.pages), batch.row_count, source.response_bytes)
        assert (entry.strike_min, entry.strike_max) == (source.strike_min, source.strike_max)
    text, digest = encode_option_cycle_input(built)
    assert decode_option_cycle_input(text, digest) == built
    assert len(text) < 8 * 1024
    assert "results" not in text and "details" not in text and "base64" not in text
    assert not any(page.response_bytes.decode("ascii")[:40] in text for row in discoveries
                   for page in row.result.batch.pages)


def test_building_rejects_empty_mixed_duplicate_and_incomplete_evidence(cycle):
    _, scope, _, evidence = flow_matrices(cycle)
    discoveries = [row.discovery for row in evidence]
    common = dict(configuration_sha256=CONFIGURATION_SHA, code_sha256=CODE_SHA, cohort_id=scope.cohort_id)
    with pytest.raises(ValueError, match="needs discovery evidence"):
        build_option_cycle_input([], **common)
    with pytest.raises(ValueError, match="unique"):
        build_option_cycle_input([discoveries[0], discoveries[0]], **common)
    other = replace(discoveries[1], result=replace(discoveries[1].result,
        batch=replace(discoveries[1].result.batch, policy_sha256=sha("another-policy"))))
    with pytest.raises(ValueError, match="share one profile, tier, scheduled cycle and policy"):
        build_option_cycle_input([discoveries[0], other], **common)
    later = replace(discoveries[1], result=replace(discoveries[1].result,
        batch=replace(discoveries[1].result.batch, scheduled_cycle=scope.cutoff.replace(minute=45))))
    with pytest.raises(ValueError, match="share one profile, tier, scheduled cycle and policy"):
        build_option_cycle_input([discoveries[0], later], **common)
    with pytest.raises(TypeError, match="complete discovery evidence"):
        build_option_cycle_input([discoveries[0].result], **common)


# --- the loader port ------------------------------------------------------------------------------------------

def test_the_loader_port_verifies_every_batch_with_three_selects_whatever_the_underlying_count():
    for count in (1, 8, 50):
        _, cycle_input, tables = world(count)
        result = verify_option_cycle_input_against_tables(tables, cycle_input)
        assert (result.batches_verified, result.batches_with_pages, result.batches_pages_pruned) == (count, count, 0)
        assert result.slot_set_verified is False
        assert len(tables.statements) == 3 and all(sql.startswith("SELECT") for sql in tables.statements)
        assert "octet_length(response_bytes)" in tables.statements[2]
        assert "SELECT discovery_batch_id::text, page_number, payload_sha256, response_bytes" not in tables.statements[2]


def test_the_loader_port_verifies_the_slot_set_header_through_the_existing_repository():
    slots = slot_set()
    _, cycle_input, tables = world(3, slots=slots)
    result = verify_option_cycle_input_against_tables(tables, cycle_input)
    assert result.slot_set_verified is True and len(tables.statements) == 5
    _, _, absent = world(3, slots=None)
    with pytest.raises(ValueError, match="slot set version is not stored"):
        verify_option_cycle_input_against_tables(absent, cycle_input)
    changed = SlotSetVersion(COHORT_ID, 1, SlotSetKind.PRE_SESSION, CYCLE_AT, sha("slot-policy"),
        (SlotSetMember(1, MarketChannel.QUOTE, SlotSetPurpose.PRIMARY, 1, 1),
         SlotSetMember(3, MarketChannel.QUOTE, SlotSetPurpose.PRIMARY, 2, 3)))
    other = FakeTables(world(3, slots=slots)[0], changed)
    with pytest.raises(ValueError, match="stored slot set members differ"):
        verify_option_cycle_input_against_tables(other, cycle_input)


def test_the_loader_port_reports_pruned_pages_and_can_require_them():
    stored, cycle_input, tables = world(4)
    for item in stored[:3]:
        item.pages.clear()
    result = verify_option_cycle_input_against_tables(tables, cycle_input)
    assert (result.batches_verified, result.batches_with_pages, result.batches_pages_pruned) == (4, 1, 3)
    with pytest.raises(ValueError, match="required but no longer stored"):
        verify_option_cycle_input_against_tables(tables, cycle_input, require_pages=True)


def test_the_loader_port_fails_closed_on_a_missing_batch():
    stored, cycle_input, tables = world(3)
    del tables.stored[stored[1].batch["discovery_batch_id"]]
    with pytest.raises(ValueError, match="SYM01 is not stored"):
        verify_option_cycle_input_against_tables(tables, cycle_input)


@pytest.mark.parametrize(("column", "value"), [
    ("evidence_sha256", sha("tampered")), ("request_filter_sha256", sha("tampered")), ("page_count", 3),
    ("row_count", 13), ("response_bytes", 1), ("expiration_from", date(2026, 10, 8)),
    ("expiration_through", date(2026, 12, 5)), ("strike_min", Decimal("85.00000001")),
    ("strike_max", Decimal("116.00000000")), ("underlyer", "OTHER"), ("underlying_security_id", str(COHORT_ID)),
    ("profile_id", str(COHORT_ID)), ("profile_sha256", sha("tampered")), ("universe_sha256", sha("tampered")),
    ("policy_sha256", sha("tampered")), ("provider_tier", "MASSIVE_ADVANCED"),
    ("scheduled_cycle", CYCLE_AT.replace(minute=15)), ("scheduled_cycle", CYCLE_AT.replace(tzinfo=None)),
])
def test_the_loader_port_fails_closed_on_any_stored_batch_difference(column, value):
    stored, cycle_input, tables = world(3)
    stored[1].batch[column] = value
    with pytest.raises(ValueError, match="differs from the cycle input: .*" + column):
        verify_option_cycle_input_against_tables(tables, cycle_input)


def test_the_loader_port_accepts_equal_decimals_and_instants_in_any_scale_or_zone():
    stored, cycle_input, tables = world(2)
    stored[0].batch["strike_min"] = Decimal("85.0")
    stored[0].batch["scheduled_cycle"] = CYCLE_AT.astimezone(timezone(timedelta(hours=-4)))
    assert verify_option_cycle_input_against_tables(tables, cycle_input).batches_verified == 2


def test_the_loader_port_fails_closed_on_coverage_and_page_differences():
    stored, cycle_input, tables = world(3)
    stored[0].coverage[4] = OptionDiscoveryCoverageBoundary("STRIKE_PRICE", "INCLUDED", "GTE", "80.00", None)
    with pytest.raises(ValueError, match="stored discovery coverage for SYM00"):
        verify_option_cycle_input_against_tables(tables, cycle_input)
    stored, cycle_input, tables = world(3)
    stored[1].coverage.pop()
    with pytest.raises(ValueError, match="stored discovery coverage for SYM01"):
        verify_option_cycle_input_against_tables(tables, cycle_input)
    stored, cycle_input, tables = world(3)
    stored[2].pages[0] = (1, sha("replaced"), stored[2].pages[0][2])
    with pytest.raises(ValueError, match="stored discovery pages for SYM02"):
        verify_option_cycle_input_against_tables(tables, cycle_input)
    stored, cycle_input, tables = world(3)
    stored[0].pages.pop()
    with pytest.raises(ValueError, match="stored discovery pages for SYM00"):
        verify_option_cycle_input_against_tables(tables, cycle_input)
    stored, cycle_input, tables = world(3)
    number, digest, size = stored[0].pages[1]
    stored[0].pages[1] = (number, digest, size + 1)
    with pytest.raises(ValueError, match="stored discovery pages for SYM00"):
        verify_option_cycle_input_against_tables(tables, cycle_input)
    stored, cycle_input, tables = world(3)
    (first_number, first_digest, first_size), (second_number, second_digest, second_size) = stored[0].pages
    stored[0].pages[:] = [(first_number, second_digest, first_size), (second_number, first_digest, second_size)]
    with pytest.raises(ValueError, match="stored discovery pages for SYM00"):
        verify_option_cycle_input_against_tables(tables, cycle_input)
    stored, cycle_input, tables = world(3)
    stored[0].pages.reverse()  # storage order is irrelevant: the query orders by page number
    assert verify_option_cycle_input_against_tables(tables, cycle_input).batches_with_pages == 3


def test_the_loader_port_agrees_with_the_builder_on_real_evidence_rows(cycle):
    _, scope, _, evidence = flow_matrices(cycle)
    discoveries = [row.discovery for row in evidence]
    built = build_option_cycle_input(discoveries, configuration_sha256=CONFIGURATION_SHA, code_sha256=CODE_SHA,
        cohort_id=scope.cohort_id)
    stored = []
    for source in discoveries:
        batch = source.result.batch
        stored.append(Stored(
            dict(discovery_batch_id=str(batch.batch_id), underlying_security_id=str(source.underlying_security_id),
                 profile_id=str(source.scope.profile_id), profile_sha256=source.scope.profile_sha256,
                 universe_sha256=source.scope.universe.sha256, provider_tier=source.scope.provider_tier.value,
                 underlyer=batch.underlyer, scheduled_cycle=batch.scheduled_cycle,
                 expiration_from=source.expiration_from, expiration_through=source.expiration_through,
                 strike_min=source.strike_min.quantize(Decimal("0.00000001")),
                 strike_max=source.strike_max.quantize(Decimal("0.00000001")),
                 request_filter_sha256=batch.request_filter_sha256, policy_sha256=batch.policy_sha256,
                 page_count=len(batch.pages), row_count=batch.row_count, response_bytes=source.response_bytes,
                 evidence_sha256=source.evidence_sha256),
            list(source.coverage),
            [(page.page_number, page.payload_sha256, len(page.response_bytes)) for page in batch.pages],
            option_cycle_underlying_from_evidence(source)))
    result = verify_option_cycle_input_against_tables(FakeTables(stored), built)
    assert result.batches_verified == len(discoveries) == result.batches_with_pages


def test_load_verified_decodes_then_verifies_and_never_queries_a_payload_that_does_not_decode():
    stored, cycle_input, tables = world(3, slots=slot_set())
    text, digest = encode_option_cycle_input(cycle_input)
    loaded, result = load_verified_option_cycle_input(tables, text, digest)
    assert loaded == cycle_input and result.batches_verified == 3 and result.slot_set_verified
    quiet = FakeTables(stored)
    with pytest.raises(ValueError, match="digest mismatch"):
        load_verified_option_cycle_input(quiet, text, sha("wrong"))
    assert quiet.statements == []
    stored[0].batch["row_count"] = 99
    with pytest.raises(ValueError, match="differs from the cycle input"):
        load_verified_option_cycle_input(tables, text, digest)


def test_the_reader_needs_a_tuple_row_cursor():
    _, cycle_input, tables = world(2)
    original = tables.fetchall
    tables.fetchall = lambda: [dict(zip(BATCH_COLUMNS, row)) for row in original()]
    with pytest.raises(TypeError, match="tuple-row cursor"):
        verify_option_cycle_input_against_tables(tables, cycle_input)
    with pytest.raises(TypeError, match="OptionCycleInput"):
        verify_option_cycle_input_against_tables(tables, {"underlyings": []})


def test_the_underlying_set_identity_has_the_reference_set_shape_and_is_order_independent():
    members = [("AAPL", UUID(int=1)), ("MSFT", UUID(int=2)), ("SPY", UUID(int=3))]
    identity = underlying_set_identity(members)
    assert identity == underlying_set_identity(list(reversed(members))) and identity.count == 3
    assert len(identity.sha256) == 64
    assert identity != underlying_set_identity(members[:2]) != underlying_set_identity(
        [("AAPL", UUID(int=1)), ("MSFT", UUID(int=2)), ("SPY", UUID(int=4))])
