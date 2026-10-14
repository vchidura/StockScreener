"""The screening generation manifest carries lineage heads, never per-bar lists, and the reader verifies them.

A stored generation's manifest listed every source bar (5.2 MB for nine names, about 226 MB for 400, in tables
with no retention yet). It now holds, per member, the lineage heads (`lineage_head.py`) of the daily bars its row
was computed from and of the hourly window behind its hourly context. The screening reader recomputes those heads
from the bars `rt.bar_current` holds and fails closed on drift, as the Ticker Workspace reader does for charts.
Offline: the fake `ScreeningStore` and an in-memory bar loader stand in for PostgreSQL.
"""
from dataclasses import replace
from datetime import date
from uuid import UUID, uuid5, NAMESPACE_URL

import pytest

from realtime import lineage_head
from realtime.contracts import canonical_json, canonical_sha256
from realtime.lineage_head import (
    BOUNDED_SCREENING_MANIFEST_BASE_BYTES, BOUNDED_SCREENING_MANIFEST_MEMBER_BYTES, manifest_bytes,
    require_bounded_screening_manifest, screening_manifest_limit,
)
from realtime.portal_compatibility_repository import RealtimeScreeningRepository, ScreeningPublicationUnavailable
from realtime.portal_projection_repository import persist_screening_generation_cursor
from realtime.screening_projection import produce_realtime_screening
from research.screening import Query, _head_membership, new_membership
from tests.realtime.test_equity_validation_history import COHORT_ID, OBSERVED_AT
from tests.realtime.test_screening_vertical_slice import ScreeningStore, current_bars_of, screening_fixture

MEMBERS = 9


@pytest.fixture(scope="module")
def inputs():
    return screening_fixture()


@pytest.fixture(scope="module")
def generation(inputs):
    return produce_realtime_screening(inputs)


def stored(generation, bar_loader):
    store = ScreeningStore()
    store.publish(generation)
    return store, RealtimeScreeningRepository(store.cursor, bar_loader=bar_loader)


def load(store, reader, **options):
    return reader._load(store, str(COHORT_ID), OBSERVED_AT, **options)


def long_lists(value, path="manifest"):
    """Every list of 12 or more scalars: a per-bar or per-revision list in a manifest."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield from long_lists(item, f"{path}.{key}")
    elif isinstance(value, list) and len(value) >= 12 and not any(isinstance(item, (dict, list)) for item in value):
        yield path


def test_the_manifest_holds_heads_and_identities_never_per_bar_lists(generation):
    manifest = dict(generation.manifest)
    assert list(long_lists(manifest)) == []
    assert manifest["lineage_contract"] == {"version": "screening_lineage_head_v1"}
    for record in manifest["lineage"].values():
        assert not {"bars", "raw_daily", "source_bars"} & set(record)
        assert set(record["bar_lineage"]["groups"]) == {"1d:ADJUSTED", "1d:RAW_UNADJUSTED"}
        assert set(record["actions"]) == set(record["split_coverage"]) == {"count", "sha256"}
    for record in manifest["hourly_lineage"].values():
        assert set(record) == {"bar_lineage", "selected_bar_id"}
        assert set(record["bar_lineage"]["groups"]) == {"1h:RAW_UNADJUSTED"}
    assert generation.manifest["input_sha256"] == lineage_head.inputs_sha256(screening_fixture())


def test_the_manifest_fits_its_cap_at_nine_names_and_at_400(generation):
    size = manifest_bytes(generation.manifest)
    assert size <= screening_manifest_limit(MEMBERS)
    # Each member's records leave headroom under the per-member allowance, but not enough for a listed history:
    # one list of 283 revision ids is about 10 KB.
    manifest = dict(generation.manifest)
    per_member = [len(canonical_json(daily)) + len(canonical_json(hourly))
                  for daily, hourly in zip(manifest["lineage"].values(), manifest["hourly_lineage"].values())]
    assert max(per_member) < BOUNDED_SCREENING_MANIFEST_MEMBER_BYTES * 0.8, per_member
    # A synthetic 400-name manifest: the nine real member records cloned under distinct securities.
    records = list(zip(manifest["lineage"].values(), manifest["hourly_lineage"].values()))
    daily, hourly = {}, {}
    for index in range(400):
        security_id = str(uuid5(NAMESPACE_URL, f"synthetic-member:{index}"))
        daily[security_id], hourly[security_id] = records[index % MEMBERS]
    manifest.update(lineage=daily, hourly_lineage=hourly, expected_members=400)
    big = manifest_bytes(require_bounded_screening_manifest(manifest, members=400))
    assert big <= screening_manifest_limit(400)
    assert 400 * 2_000 < big < 400 * BOUNDED_SCREENING_MANIFEST_MEMBER_BYTES
    # The same 400 members with one listed history per member (what the manifest used to carry) are refused.
    listed = {security_id: dict(record, bars=[str(uuid5(NAMESPACE_URL, f"bar:{security_id}:{n}")) for n in range(283)])
              for security_id, record in daily.items()}
    with pytest.raises(ValueError, match="must stay within"):
        require_bounded_screening_manifest(dict(manifest, lineage=listed), members=400)
    assert BOUNDED_SCREENING_MANIFEST_BASE_BYTES == 24 * 1024


def test_the_producer_fails_when_its_manifest_outgrows_the_cap(inputs, monkeypatch):
    monkeypatch.setattr(lineage_head, "BOUNDED_SCREENING_MANIFEST_MEMBER_BYTES", 256)
    monkeypatch.setattr(lineage_head, "BOUNDED_SCREENING_MANIFEST_BASE_BYTES", 1024)
    with pytest.raises(ValueError, match="screening generation manifest is .* must stay within"):
        produce_realtime_screening(inputs)


def test_the_writer_refuses_an_oversize_manifest_before_any_statement(generation):
    store = ScreeningStore()
    bloated = replace(generation, manifest=dict(generation.manifest, listed_bars=["0" * 36] * 5000))
    with pytest.raises(ValueError, match="screening generation manifest is .* must stay within"):
        persist_screening_generation_cursor(store, bloated, values_executor=store.values)
    assert store.statements == [] and store.generations == {}


def test_a_row_is_a_few_kib_and_the_identity_is_the_compact_manifests(generation):
    from research.screening import digest

    assert max(len(canonical_json(row.payload)) for row in generation.rows) < 2_600
    payload = dict(generation.manifest, rows=[row.payload for row in generation.rows])
    assert digest({key: value for key, value in payload.items() if key != "generation"}) == generation.manifest["generation"]


def test_the_reader_verifies_every_head_against_the_current_bars_once_per_generation(inputs, generation):
    calls = []
    loader = current_bars_of(inputs)

    def counting(cursor, **options):
        calls.append(len(options["targets"]))
        return loader(cursor, **options)

    store, reader = stored(generation, counting)
    payload = load(store, reader)
    # Nine members, two daily groups and one hourly group each.
    assert calls == [MEMBERS * 3] and len(payload["rows"]) == MEMBERS
    load(store, reader)
    assert calls == [MEMBERS * 3]
    # A comparison's previous generation is read for its heads only: never verified against the bars.
    load(store, reader, verify_bars=False)
    assert calls == [MEMBERS * 3]


def test_a_large_cohort_is_verified_in_bounded_chunks_of_members(inputs, generation, monkeypatch):
    import realtime.portal_compatibility_repository as repository_module

    monkeypatch.setattr(repository_module, "VERIFICATION_CHUNK_MEMBERS", 4)
    calls = []
    loader = current_bars_of(inputs)

    def counting(cursor, **options):
        calls.append(len(options["targets"]))
        return loader(cursor, **options)

    store, reader = stored(generation, counting)
    load(store, reader)
    assert calls == [12, 12, 3]


def drifted_loader(inputs, ticker, mutate):
    bars = [dict(bar) for bar in inputs.bars]
    mutate(next(bar for bar in bars if bar["ticker"] == ticker and bar["interval"] == "1d"
                and bar["adjustment_basis"] == "ADJUSTED" and bar["session_date"] == date(2026, 9, 18)), bars)
    return current_bars_of(replace(inputs, bars=tuple(bars)))


@pytest.mark.parametrize("defect", ["revised", "retired", "hourly_revised", "hourly_retired", "raw_revised"])
def test_the_reader_fails_closed_when_a_published_bar_was_revised_or_retired(inputs, generation, defect):
    def mutate(daily, bars):
        if defect == "revised":
            daily["payload_sha256"] = "e" * 64
            daily["bar_revision_id"] = uuid5(NAMESPACE_URL, "revised")
        elif defect == "retired":
            bars.remove(daily)
        elif defect == "raw_revised":
            raw = max((b for b in bars if b["ticker"] == daily["ticker"] and b["interval"] == "1d"
                       and b["adjustment_basis"] == "RAW_UNADJUSTED"), key=lambda b: b["bar_start"])
            raw["payload_sha256"] = "d" * 64
        else:
            hourly = max((b for b in bars if b["ticker"] == daily["ticker"] and b["interval"] == "1h"), key=lambda b: b["bar_end"])
            if defect == "hourly_revised":
                hourly["payload_sha256"] = "c" * 64
            else:
                bars.remove(hourly)

    store, reader = stored(generation, drifted_loader(inputs, "AAPL", mutate))
    with pytest.raises(ScreeningPublicationUnavailable, match="republish required"):
        load(store, reader)
    # Nothing is cached for a generation that failed, and a previous generation is still readable for its heads.
    with pytest.raises(ScreeningPublicationUnavailable):
        load(store, reader)
    assert load(store, reader, verify_bars=False)["rows"]


def test_bars_appended_after_the_head_do_not_disturb_the_generation(inputs, generation):
    later = dict(next(b for b in inputs.bars if b["ticker"] == "AAPL" and b["interval"] == "1h"))
    last = max((b for b in inputs.bars if b["ticker"] == "AAPL" and b["interval"] == "1h"), key=lambda b: b["bar_end"])
    later.update(bar_revision_id=uuid5(NAMESPACE_URL, "later"), bar_start=last["bar_end"],
                 bar_end=last["bar_end"] + (last["bar_end"] - last["bar_start"]))
    store, reader = stored(generation, current_bars_of(replace(inputs, bars=(*inputs.bars, later))))
    assert load(store, reader)["rows"]


def test_without_a_bar_loader_only_the_stored_identity_checks_run(generation):
    store, reader = stored(generation, None)
    assert load(store, reader)["rows"]
    with pytest.raises(ScreeningPublicationUnavailable, match="bar tables"):
        reader._compose_hourly_lineage(load(store, reader), next(iter(generation.manifest["lineage"])),
                                       OBSERVED_AT, COHORT_ID)


def head_pair(generation, ticker):
    security_id = next(row.security_id for row in generation.rows if row.ticker == ticker)
    record = generation.manifest["lineage"][str(security_id)]
    row = next(row.payload for row in generation.rows if row.ticker == ticker)
    return row, record


def test_new_membership_compares_the_heads_of_consecutive_generations(generation, inputs):
    row, record = head_pair(generation, "AAPL")
    # The previous generation as it would look one session earlier: its latest bar is the current oldest-shifted one.
    previous = dict(
        record, current_quote_bar_id=record["previous_quote_bar_id"],
        adjusted_without_oldest_sha256=record["adjusted_without_latest_sha256"],
    )
    prior = dict(row, source_bar_id=previous["current_quote_bar_id"])
    assert _head_membership("NO_MATCH", row, prior, record, previous) == ("NEW", "PRIOR_NONMATCH")
    assert _head_membership("MATCH", row, prior, record, previous) == ("NOT_NEW", "PRIOR_MATCH")
    for change, reason in (
        (dict(adjusted_without_oldest_sha256="f" * 64), "SOURCE_HISTORY_CHANGED"),
        (dict(actions={"count": 1, "sha256": "f" * 64}), "SOURCE_HISTORY_CHANGED"),
        (dict(current_quote_bar_id="x"), "SOURCE_LINEAGE_UNAVAILABLE"),
        (dict(historical_price_basis="RAW"), "SOURCE_HISTORY_CHANGED"),
    ):
        outcome = _head_membership("NO_MATCH", row, prior, record, dict(previous, **change))
        assert outcome == ("UNAVAILABLE", reason), change
    # Lists stored by an older generation are not compared against heads: the comparison is unavailable.
    legacy = dict(bars=["a"], raw_daily=["a", "b"], actions=[], historical_price_basis="PROVIDER_SPLIT_ADJUSTED",
                  current_quote_bar_id="a")
    outcome = new_membership(row, prior, Query().predicate, record, legacy)
    assert outcome[0] == "UNAVAILABLE"
