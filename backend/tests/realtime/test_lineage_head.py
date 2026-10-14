from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.lineage_head import (
    BOUNDED_MANIFEST_BYTES, BOUNDED_WORKSPACE_PAYLOAD_BYTES, bar_lineage_head, inputs_bar_lineage_sha256,
    inputs_lineage, inputs_sha256, manifest_bytes, reference_set, require_bounded_manifest,
    require_bounded_payload, require_member_set, revision_chain_sha256, rows_identity, workspace_set,
)
from realtime.preflight import ProviderTier
from realtime.screening_projection import RealtimeScreeningInput

START = datetime(2026, 10, 2, 13, 30, tzinfo=timezone.utc)


def _bar(ticker, interval, index, basis="RAW_UNADJUSTED", payload="a"):
    begin = START + timedelta(minutes=5 * index)
    return dict(
        ticker=ticker, interval=interval, adjustment_basis=basis, bar_start=begin,
        bar_end=begin + timedelta(minutes=5),
        bar_revision_id=uuid5(NAMESPACE_URL, f"{ticker}:{interval}:{basis}:{index}"),
        payload_sha256=payload * 64,
    )


def _inputs(bars, members=()):
    return RealtimeScreeningInput(
        cohort_id=uuid5(NAMESPACE_URL, "cohort"), provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        source_manifest_sha256="b" * 64, source_generation=3, session=date(2026, 10, 2), cutoff=START,
        members=tuple(members), bars=tuple(bars), actions=(), coverage=(),
    )


def test_chain_digest_is_order_payload_and_membership_sensitive():
    pairs = [(f"id-{index}", "a" * 64) for index in range(5)]
    base = revision_chain_sha256(pairs)
    assert base == revision_chain_sha256(list(pairs))
    assert base != revision_chain_sha256(pairs[::-1])
    assert base != revision_chain_sha256(pairs[:-1])
    assert base != revision_chain_sha256(pairs + [("id-5", "a" * 64)])
    assert base != revision_chain_sha256(pairs[:2] + [("id-2", "c" * 64)] + pairs[3:])
    assert base != revision_chain_sha256(pairs[:2] + [("other", "a" * 64)] + pairs[3:])


def test_lineage_head_is_bounded_by_group_count_not_bar_count():
    rows = [_bar("AAPL", "5m", index) for index in range(14_000)] + [_bar("AAPL", "1d", 0, "ADJUSTED")]
    head = bar_lineage_head(rows)
    assert head["bars"] == 14_001 and set(head["groups"]) == {"5m:RAW_UNADJUSTED", "1d:ADJUSTED"}
    group = head["groups"]["5m:RAW_UNADJUSTED"]
    assert group["count"] == 14_000
    assert group["first_revision_id"] == str(rows[0]["bar_revision_id"])
    assert group["last_revision_id"] == str(rows[13_999]["bar_revision_id"])
    assert manifest_bytes(head) < 2048


def test_lineage_head_ignores_row_order_and_detects_any_revision_change():
    rows = [_bar("AAPL", "5m", index) for index in range(40)]
    head = bar_lineage_head(rows)
    assert bar_lineage_head(rows[::-1]) == head
    revised = rows[:10] + [dict(rows[10], payload_sha256="c" * 64)] + rows[11:]
    assert bar_lineage_head(revised)["sha256"] != head["sha256"]
    assert bar_lineage_head(rows[:-1])["sha256"] != head["sha256"]


def test_require_bounded_manifest_rejects_an_oversized_manifest():
    assert require_bounded_manifest({"small": "x"}, name="test") == {"small": "x"}
    with pytest.raises(ValueError, match="lineage-head manifest must stay within"):
        require_bounded_manifest({"rows": ["x" * 100] * 200}, name="test", limit=BOUNDED_MANIFEST_BYTES)


def test_require_bounded_payload_rejects_embedded_bar_bodies():
    compact = {"chart_refs": {"5m": "5m:RAW_UNADJUSTED"}, "quote": {"price": 1.0}}
    assert require_bounded_payload(compact, name="ticker") == compact
    # 14,000 five-minute points embedded again would be about 1.2 MB.
    points = [dict(time=index, open=1.0, high=2.0, low=0.5, close=1.5, volume=100) for index in range(14_000)]
    with pytest.raises(ValueError, match="by-reference payload must stay within"):
        require_bounded_payload(dict(compact, charts={"5m": points}), name="ticker")
    assert BOUNDED_WORKSPACE_PAYLOAD_BYTES == 128 * 1024


def test_member_sets_are_order_independent_and_constant_size():
    members = [dict(ticker=name, security_revision_id=uuid5(NAMESPACE_URL, name), payload="x" * 400)
               for name in ("MSFT", "AAPL", "NVDA")]
    identity = reference_set(members)
    assert identity == reference_set(members[::-1]) and identity["count"] == 3
    assert reference_set(members + members * 100)["count"] == 303
    assert manifest_bytes(reference_set(members * 1000)) <= manifest_bytes(identity) + 3
    changed = [dict(members[0], payload="y" * 400)] + members[1:]
    assert reference_set(changed) != identity
    with pytest.raises(ValueError, match="member-set identity drift"):
        require_member_set(identity, reference_set(changed), name="Test")
    workspaces = [dict(ticker=name, snapshot_id=str(uuid5(NAMESPACE_URL, name)), payload_sha256="d" * 64)
                  for name in ("B", "A")]
    assert workspace_set(workspaces) == workspace_set(workspaces[::-1])
    assert rows_identity([]) == dict(count=0, sha256=rows_identity([])["sha256"])


def test_inputs_digest_tracks_bars_once_per_object_and_never_serializes_rows():
    rows = [_bar(name, "5m", index) for name in ("AAPL", "MSFT") for index in range(30)]
    inputs = _inputs(rows)
    digest = inputs_sha256(inputs)
    assert digest == inputs_sha256(inputs) and inputs.__dict__["_derived_cache"]["inputs_sha256"] == digest
    assert set(inputs_lineage(inputs)) == {"AAPL", "MSFT"}
    assert inputs_bar_lineage_sha256(inputs) == inputs_bar_lineage_sha256(_inputs(rows[::-1]))
    assert inputs_sha256(_inputs(rows)) == digest
    revised = list(rows)
    revised[3] = dict(rows[3], payload_sha256="c" * 64)
    assert inputs_sha256(_inputs(revised)) != digest
    assert inputs_sha256(replace(inputs, cutoff=START + timedelta(minutes=5))) != digest
    # a changed copy does not inherit the cached digest
    assert inputs_sha256(replace(inputs, bars=tuple(revised))) != digest


def test_one_tickers_revision_leaves_the_other_tickers_head_unchanged():
    rows = [_bar(name, "5m", index) for name in ("AAPL", "MSFT") for index in range(30)]
    before = inputs_lineage(_inputs(rows))
    revised = [dict(row, payload_sha256="c" * 64) if row["ticker"] == "AAPL" else row for row in rows]
    after = inputs_lineage(_inputs(revised))
    assert before["MSFT"] == after["MSFT"] and before["AAPL"]["sha256"] != after["AAPL"]["sha256"]
