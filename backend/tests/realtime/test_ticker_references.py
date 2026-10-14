"""Ticker Workspace payloads by reference: the pure composition and verification the reader relies on."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from random import Random
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from realtime.contracts import canonical_sha256
from realtime.lineage_head import bar_lineage_head
from realtime.ticker_references import (
    BarLineageChanged,
    chart_basis,
    chart_group_key,
    chart_group_ranges,
    chart_points,
    compose_charts,
    compose_evidence,
    embedded_evidence_row,
    embedded_evidence_row_of,
    evidence_expectations,
    evidence_ref,
    verify_chart_group,
)
from tests.realtime.test_equity_evidence_repository import _setup_result
from tests.realtime.test_ticker_vertical_slice import _old_evidence_row

START = datetime(2026, 10, 2, 13, 30, tzinfo=timezone.utc)
GROUP = "5m:RAW_UNADJUSTED"


def _rows(count=40, interval="5m", basis="RAW_UNADJUSTED", zone=timezone.utc):
    rows = []
    for index in range(count):
        begin = START + timedelta(minutes=5 * index)
        rows.append(dict(
            interval=interval, adjustment_basis=basis,
            bar_revision_id=uuid5(NAMESPACE_URL, f"{interval}:{index}"),
            bar_start=begin.astimezone(zone), bar_end=(begin + timedelta(minutes=5)).astimezone(zone),
            open_price=Decimal("10.00600000") + index, high_price=Decimal("12.00000000") + index,
            low_price=Decimal("9.99499999") + index, close_price=Decimal("10.50000000") + index,
            volume=Decimal("1200.00000000") + index, payload_sha256=f"{index:064x}",
        ))
    return rows


def _manifest(rows):
    return dict(bar_lineage=bar_lineage_head(rows))


def test_chart_intervals_map_to_their_lineage_head_groups():
    assert {interval: chart_basis(interval) for interval in ("1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo")} == {
        "1m": "RAW_UNADJUSTED", "5m": "RAW_UNADJUSTED", "15m": "RAW_UNADJUSTED", "30m": "RAW_UNADJUSTED",
        "1h": "RAW_UNADJUSTED", "1d": "ADJUSTED", "1wk": "ADJUSTED", "1mo": "ADJUSTED"}
    assert chart_group_key("1d") == "1d:ADJUSTED" and chart_group_key("5m") == GROUP


def test_chart_points_equal_the_arithmetic_the_producer_applies_through_the_float8_loader():
    rng = Random(20261013)
    rows = []
    for index in range(20_000):
        numeric = Decimal(rng.randrange(1, 10 ** 12)) / 10 ** 8
        rows.append(dict(
            bar_start=START + timedelta(minutes=index), open_price=numeric, high_price=numeric,
            low_price=numeric, close_price=numeric, volume=Decimal(rng.randrange(0, 10 ** 13)) / 10 ** 3,
        ))
    # The loader reads `open_price::float8`; the producer then builds `Decimal(str(float))` facts and
    # `_chart_point` rounds `float(...)`. The reader rounds the numeric directly and must agree.
    for point, row in zip(chart_points(rows), rows):
        producer = float(Decimal(str(float(row["open_price"]))))
        assert point == dict(
            time=int(row["bar_start"].timestamp()), open=round(producer, 2), high=round(producer, 2),
            low=round(producer, 2), close=round(producer, 2),
            volume=int(Decimal(str(float(row["volume"])))),
        )


def test_a_group_that_matches_its_head_composes_into_chart_points():
    rows = _rows()
    manifest = _manifest(rows)
    charts = compose_charts(manifest, {"5m": GROUP}, {"5m": rows[-1]["bar_end"].isoformat()}, {GROUP: rows})
    assert charts == {"5m": chart_points(rows)}
    assert charts["5m"][0] == dict(
        time=int(START.timestamp()), open=10.01, high=12.0, low=9.99, close=10.5, volume=1200)


def test_head_instants_compare_across_time_zone_representations():
    rows = _rows(zone=timezone(timedelta(hours=-4)))
    head = bar_lineage_head(_rows())["groups"][GROUP]
    assert rows[0]["bar_start"].isoformat() != head["first_bar_start"]
    verify_chart_group(head, rows)


@pytest.mark.parametrize("defect", ["revised_id", "revised_hash", "dropped_last", "dropped_first", "dropped_middle",
                                     "appended", "reordered", "empty"])
def test_a_changed_group_fails_closed_as_a_lineage_change(defect):
    rows = _rows()
    head = bar_lineage_head(rows)["groups"][GROUP]
    if defect == "revised_id":
        rows[7] = dict(rows[7], bar_revision_id=uuid4())
    elif defect == "revised_hash":
        rows[-1] = dict(rows[-1], payload_sha256="f" * 64)
    elif defect == "dropped_last":
        rows = rows[:-1]
    elif defect == "dropped_first":
        rows = rows[1:]
    elif defect == "dropped_middle":
        rows = rows[:20] + rows[21:]
    elif defect == "appended":
        rows = rows + _rows(41)[40:]
    elif defect == "reordered":
        rows[10], rows[11] = rows[11], rows[10]
    else:
        rows = []
    with pytest.raises(BarLineageChanged, match="lineage changed"):
        verify_chart_group(head, rows)


def test_chart_references_are_checked_against_the_manifest_head():
    rows = _rows()
    manifest = _manifest(rows)
    first, last = rows[0]["bar_start"], rows[-1]["bar_end"]
    assert chart_group_ranges(manifest, {"5m": GROUP}) == {GROUP: (first, last)}
    tampered = dict(bar_lineage=dict(manifest["bar_lineage"], sha256="0" * 64))
    with pytest.raises(ValueError, match="head identity drift"):
        chart_group_ranges(tampered, {"5m": GROUP})
    with pytest.raises(ValueError, match="chart reference identity drift"):
        chart_group_ranges(manifest, {"5m": "15m:RAW_UNADJUSTED"})
    with pytest.raises(ValueError, match="chart reference identity drift"):
        chart_group_ranges(manifest, {"1d": GROUP})
    with pytest.raises(ValueError, match="market time identity drift"):
        compose_charts(manifest, {"5m": GROUP}, {"5m": (last + timedelta(minutes=5)).isoformat()}, {GROUP: rows})


def test_evidence_rows_embed_in_the_shape_the_payload_used_to_carry_them():
    result = _setup_result()
    for row in result.materialization.evidence:
        assert embedded_evidence_row_of(row) == _old_evidence_row(row) | {"quality_codes": list(row.quality_codes)}
    row = result.materialization.evidence[0]
    # Stored timestamps arrive in the connection's zone; the embedded form is always UTC.
    zoned = embedded_evidence_row(
        evidence_id=row.evidence_id, payload_sha256=row.payload_sha256, payload={},
        source_revision_ids=row.source_revision_ids,
        market_time=row.market_time.astimezone(timezone(timedelta(hours=-4))),
        observed_at=row.observed_at.astimezone(timezone(timedelta(hours=5))), quality_codes=["X"],
        evidence_type=row.evidence_type.value, analysis_run_id=row.analysis_run_id,
        security_revision_id=row.security_revision_id, latest_bar_revision_id=None, source_name="n",
        source_version="v", payload_schema_version="1", valid_until=None, quality_state="OK",
        evidence_role="BODY", fundamental_report_ids=(),
    )
    assert zoned["market_time"] == row.market_time.isoformat() and zoned["observed_at"] == row.observed_at.isoformat()
    assert zoned["valid_until"] is None and zoned["latest_bar_revision_id"] == "None"


def test_evidence_index_pins_each_id_to_one_identity_and_composes_in_publication_order():
    result = _setup_result()
    evidence = list(result.materialization.evidence)
    refs = [evidence_ref(row) for row in evidence]
    assert refs[0] == dict(evidence_id=str(evidence[0].evidence_id), evidence_type=evidence[0].evidence_type.value,
                           payload_sha256=evidence[0].payload_sha256)
    index = {"first": refs, "second": refs[:2][::-1]}
    expected = evidence_expectations(index)
    assert expected == {row.evidence_id: (row.evidence_type.value, row.payload_sha256) for row in evidence}
    with pytest.raises(ValueError, match="reference identity conflict"):
        evidence_expectations({"first": refs, "second": [dict(refs[0], payload_sha256="0" * 64)]})
    bodies = {row.evidence_id: dict(
        evidence_id=row.evidence_id, payload_sha256=row.payload_sha256, payload=None, source_revision_ids=(),
        market_time=row.market_time, observed_at=row.observed_at, quality_codes=(),
        evidence_type=row.evidence_type.value, analysis_run_id=None, security_revision_id=None,
        latest_bar_revision_id=None, source_name="", source_version="", payload_schema_version="",
        valid_until=None, quality_state="", evidence_role="", fundamental_report_ids=(),
    ) for row in evidence}
    composed = compose_evidence(index, bodies)
    assert [row["evidence_id"] for row in composed["first"]] == [str(row.evidence_id) for row in evidence]
    assert [row["evidence_id"] for row in composed["second"]] == [str(evidence[1].evidence_id), str(evidence[0].evidence_id)]
    assert canonical_sha256(index) == canonical_sha256({key: list(value) for key, value in index.items()})
    assert UUID(refs[0]["evidence_id"]) == evidence[0].evidence_id
