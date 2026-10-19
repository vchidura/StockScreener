import gzip
import hashlib
import json
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from realtime.session_archive import (
    ALERT_KINDS,
    ARCHIVE_SCHEMA_VERSION,
    DETECTOR_EVALUATIONS,
    DROP,
    FLOW_KINDS,
    KEEP,
    KIND_RELATIONS,
    OPTION_FLOW_CLUSTERS,
    OPTION_FLOW_EVENTS,
    OUTCOMES,
    PIN_OPEN_HORIZON,
    PIN_OUTCOME_UNVERIFIED,
    REASON_ARCHIVE_MISSING,
    REASON_ARCHIVE_UNVERIFIED,
    REASON_ARCHIVES_VERIFIED,
    REASON_NO_SESSIONS,
    REASON_OPEN_PIN,
    REASON_WITHIN_ONLINE_WINDOW,
    SESSION_KINDS,
    STOCK_ALERTS,
    ArchiveIdentity,
    ArchiveRecord,
    ArchiveRow,
    ArchiveVerificationError,
    PartitionRange,
    Pin,
    VerificationCode,
    _write_new_file,
    archive_directory,
    archive_file_names,
    build_archive,
    decide_partition_drop,
    render_manifest,
    summarize_clocks,
    to_jsonable,
    verify_archive,
    write_archive,
)

UTC = timezone.utc
SESSION = date(2026, 10, 5)
HORIZON = date(2026, 10, 8)
IDENTITY = ArchiveIdentity("build-1", "policy-1")
VERIFIED_AT = datetime(2026, 10, 5, 21, 30, tzinfo=UTC)


def _stamp(minute):
    return datetime(2026, 10, 5, 17, minute, tzinfo=UTC)


def _alert_rows(count=3):
    rows = []
    for index in range(count):
        alert_id = f"a0000000-0000-0000-0000-00000000000{index}"
        event_id = f"e0000000-0000-0000-0000-00000000000{index}"
        rows.append(ArchiveRow("rt.alert_current", (alert_id,), {
            "alert_id": UUID(alert_id), "event_id": UUID(event_id), "alert_kind": "STOCK",
            "as_of": _stamp(index), "updated_at": _stamp(index + 10),
            "projection": {"entry": Decimal("101.50"), "levels": [1, 2.5, None], "note": "caf\u00e9"},
        }))
        rows.append(ArchiveRow("rt.detector_events", (event_id,), {
            "event_id": UUID(event_id), "trigger_at": _stamp(index), "decision_at": _stamp(index + 1),
            "created_at": _stamp(index + 2), "reason_codes": ["A", "B"],
        }))
        rows.append(ArchiveRow("rt.detector_event_inputs", (event_id, "entry", "k1"), {
            "event_id": UUID(event_id), "available_at": _stamp(index), "received_at": _stamp(index + 1),
            "retained_input": {"price": 101.5},
        }))
    return rows


def _build(rows=None, kind=STOCK_ALERTS, horizon=None, session=SESSION):
    return build_archive(kind, session, _alert_rows() if rows is None else rows, horizon_close_date=horizon)


def _written(tmp_path, built=None, identity=IDENTITY):
    built = built or _build()
    directory = archive_directory(tmp_path, built.session_date)
    return built, write_archive(directory, built, identity)


def _lines(built):
    return [json.loads(line) for line in gzip.decompress(built.payload).splitlines()]


def _rewrite_manifest(path, **changes):
    manifest = json.loads(path.read_text(encoding="ascii"))
    manifest.update(changes)
    path.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="ascii")


def _verify(written, built, **overrides):
    arguments = dict(kind=built.kind, session_date=built.session_date,
                     horizon_close_date=built.horizon_close_date, source_counts=dict(built.relation_counts))
    arguments.update(overrides)
    return verify_archive(written.data_path, written.manifest_path, **arguments)


# Format -----------------------------------------------------------------------------------


def test_bytes_are_deterministic_and_independent_of_input_order():
    rows = _alert_rows(5)
    first = _build(rows)
    shuffled = list(rows)
    random.Random(7).shuffle(shuffled)
    second = _build(shuffled)
    assert first.payload == second.payload
    assert first.sha256 == second.sha256 == hashlib.sha256(first.payload).hexdigest()
    assert first.payload[4:8] == b"\x00\x00\x00\x00"  # gzip mtime is fixed


def test_lines_follow_relation_order_then_key_and_keep_stored_values():
    built = _build(list(reversed(_alert_rows(2))))
    lines = _lines(built)
    assert [line["relation"] for line in lines] == [
        "rt.alert_current", "rt.alert_current", "rt.detector_events", "rt.detector_events",
        "rt.detector_event_inputs", "rt.detector_event_inputs",
    ]
    first = lines[0]["row"]
    assert first["alert_id"] == "a0000000-0000-0000-0000-000000000000"
    assert first["as_of"] == "2026-10-05T17:00:00.000000Z"
    assert first["projection"] == {"entry": "101.50", "levels": [1, 2.5, None], "note": "caf\u00e9"}
    assert lines[2]["row"]["reason_codes"] == ["A", "B"]
    assert built.row_count == 6
    assert built.relation_counts == {"rt.alert_current": 2, "rt.detector_events": 2, "rt.detector_event_inputs": 2}


def test_empty_session_still_produces_a_verifiable_file(tmp_path):
    built = _build(rows=[])
    assert built.row_count == 0 and gzip.decompress(built.payload) == b""
    written = write_archive(archive_directory(tmp_path, SESSION), built, IDENTITY)
    assert _verify(written, built).row_count == 0


@pytest.mark.parametrize("value", [datetime(2026, 10, 5, 17, 0), float("nan"), b"raw", {1: "x"}, {"x": {2: 3}}])
def test_unserializable_values_are_rejected(value):
    with pytest.raises((ValueError, TypeError)):
        to_jsonable(value)


def test_non_utc_timestamp_is_normalized_to_utc():
    zone = timezone(timedelta(hours=-4))
    assert to_jsonable(datetime(2026, 10, 5, 13, 0, tzinfo=zone)) == "2026-10-05T17:00:00.000000Z"


def test_duplicate_unknown_and_misplaced_rows_are_rejected():
    rows = _alert_rows(1)
    with pytest.raises(ValueError, match="duplicate"):
        _build(rows + [rows[0]])
    with pytest.raises(ValueError, match="not part of"):
        _build([ArchiveRow("rt.detector_datasets", ("d",), {"x": 1})])
    with pytest.raises(ValueError, match="horizon"):
        _build(rows=[], kind=OUTCOMES)
    with pytest.raises(ValueError, match="horizon"):
        _build(rows=[], horizon=HORIZON)


# Manifest ----------------------------------------------------------------------------------


def test_manifest_records_counts_hashes_identity_and_the_four_clocks(tmp_path):
    built, written = _written(tmp_path)
    manifest = json.loads(written.manifest_path.read_text(encoding="ascii"))
    assert manifest["schema_version"] == ARCHIVE_SCHEMA_VERSION
    assert manifest["sha256"] == hashlib.sha256(written.data_path.read_bytes()).hexdigest()
    assert manifest["content_sha256"] == hashlib.sha256(gzip.decompress(built.payload)).hexdigest()
    assert manifest["row_count"] == 9 and manifest["relation_counts"]["rt.alert_current"] == 3
    assert (manifest["build_id"], manifest["policy_id"]) == ("build-1", "policy-1")
    assert manifest["archive_kind"] == STOCK_ALERTS and manifest["session_date"] == "2026-10-05"
    assert manifest["file"] == "stock_alerts.jsonl.gz" and manifest["horizon_close_date"] is None
    clocks = manifest["clocks"]
    assert set(clocks) == {"market", "observed", "durable", "replay"}
    assert clocks["market"]["min"] == "2026-10-05T17:00:00.000000Z"
    assert clocks["market"]["max"] == "2026-10-05T17:02:00.000000Z"
    assert clocks["market"]["columns"] == ["rt.detector_event_inputs.available_at", "rt.detector_events.trigger_at"]
    assert clocks["durable"]["max"] == "2026-10-05T17:12:00.000000Z"
    assert clocks["replay"] is None


def test_manifest_bytes_are_reproducible_and_hold_no_wall_clock():
    built = _build()
    first = render_manifest(built, file_name="stock_alerts.jsonl.gz", identity=IDENTITY)
    assert first == render_manifest(built, file_name="stock_alerts.jsonl.gz", identity=IDENTITY)
    assert b"verified_at" not in first and b"written" not in first


def test_clock_summary_ignores_missing_values():
    assert summarize_clocks([("rt.detector_evaluations", {"observed_at": None})])["observed"] is None
    summary = summarize_clocks([("rt.detector_evaluations", {"observed_at": "2026-10-05T17:00:00.000000Z"})])
    assert summary["observed"]["columns"] == ["rt.detector_evaluations.observed_at"]


def test_identity_is_validated():
    with pytest.raises(ValueError):
        ArchiveIdentity("  ", "policy")
    with pytest.raises(ValueError):
        ArchiveIdentity("build", "")
    with pytest.raises(ValueError):
        ArchiveIdentity("b" * 97, "policy")


# Writing: atomic, idempotent, append-only -------------------------------------------------


def test_write_leaves_only_final_files(tmp_path):
    _, written = _written(tmp_path)
    assert written.created and not written.revision
    assert sorted(path.name for path in written.data_path.parent.iterdir()) == [
        "stock_alerts.jsonl.gz", "stock_alerts.manifest.json"]


def test_rerun_is_idempotent_and_does_not_touch_files(tmp_path):
    built, first = _written(tmp_path)
    stat = first.data_path.stat()
    manifest_bytes = first.manifest_path.read_bytes()
    again = write_archive(first.data_path.parent, _build(), IDENTITY)
    assert not again.created and not again.revision
    assert again.data_path == first.data_path
    assert first.data_path.stat().st_mtime_ns == stat.st_mtime_ns
    assert first.manifest_path.read_bytes() == manifest_bytes
    assert len(list(first.data_path.parent.iterdir())) == 2


def test_rerun_with_a_new_build_identity_keeps_the_original_manifest(tmp_path):
    _, first = _written(tmp_path)
    again = write_archive(first.data_path.parent, _build(), ArchiveIdentity("build-2", "policy-2"))
    manifest = json.loads(again.manifest_path.read_text(encoding="ascii"))
    assert manifest["build_id"] == "build-1"


def test_missing_manifest_is_completed_after_an_interrupted_write(tmp_path):
    built, first = _written(tmp_path)
    first.manifest_path.unlink()
    again = write_archive(first.data_path.parent, built, IDENTITY)
    assert not again.created and again.manifest_path.exists()
    _verify(again, built)


def test_changed_rows_never_rewrite_the_original_file(tmp_path):
    built, first = _written(tmp_path)
    original = first.data_path.read_bytes()
    changed = _build(_alert_rows(4))
    second = write_archive(first.data_path.parent, changed, IDENTITY)
    assert second.revision and second.created
    assert second.data_path.name == f"stock_alerts.{changed.content_sha256[:16]}.jsonl.gz"
    assert first.data_path.read_bytes() == original
    assert _verify(first, built).sha256 == built.sha256
    assert _verify(second, changed).sha256 == changed.sha256 != built.sha256
    third = write_archive(first.data_path.parent, changed, IDENTITY)
    assert third.data_path == second.data_path and not third.created


def test_existing_file_is_never_replaced_by_the_atomic_writer(tmp_path):
    target = tmp_path / "existing.jsonl.gz"
    target.write_bytes(b"original")
    with pytest.raises(FileExistsError):
        _write_new_file(target, b"replacement")
    assert target.read_bytes() == b"original"
    assert [path.name for path in tmp_path.iterdir()] == ["existing.jsonl.gz"]


def test_unreadable_existing_file_is_preserved_and_content_goes_to_a_revision(tmp_path):
    directory = archive_directory(tmp_path, SESSION)
    directory.mkdir(parents=True)
    (directory / "stock_alerts.jsonl.gz").write_bytes(b"not gzip")
    written = write_archive(directory, _build(), IDENTITY)
    assert written.revision
    assert (directory / "stock_alerts.jsonl.gz").read_bytes() == b"not gzip"


def test_outcome_addenda_are_append_only_per_horizon_close_date(tmp_path):
    def outcome_rows(count):
        return [ArchiveRow("rt.alert_management_assessments", (f"m{index}",), {
            "assessment_id": f"m{index}", "assessed_at": _stamp(index), "status": "TARGET"})
            for index in range(count)]

    first = build_archive(OUTCOMES, SESSION, outcome_rows(2), horizon_close_date=HORIZON)
    directory = archive_directory(tmp_path, SESSION)
    one = write_archive(directory, first, IDENTITY)
    assert one.data_path.name == "outcomes-2026-10-08.jsonl.gz"
    assert one.manifest_path.name == "outcomes-2026-10-08.manifest.json"
    original = one.data_path.read_bytes()
    second = build_archive(OUTCOMES, SESSION, outcome_rows(3), horizon_close_date=HORIZON)
    two = write_archive(directory, second, IDENTITY)
    assert two.revision and two.data_path != one.data_path
    assert second.sha256 != first.sha256
    assert one.data_path.read_bytes() == original
    other_day = build_archive(OUTCOMES, SESSION, outcome_rows(2), horizon_close_date=date(2026, 10, 9))
    assert write_archive(directory, other_day, IDENTITY).data_path.name == "outcomes-2026-10-09.jsonl.gz"
    assert json.loads(one.manifest_path.read_text(encoding="ascii"))["horizon_close_date"] == "2026-10-08"


def test_file_names():
    assert archive_file_names(DETECTOR_EVALUATIONS) == ("detector_evaluations.jsonl.gz",
                                                        "detector_evaluations.manifest.json")
    assert archive_file_names(OPTION_FLOW_EVENTS)[0] == "option_flow_events.jsonl.gz"
    assert archive_file_names(OPTION_FLOW_CLUSTERS)[1] == "option_flow_clusters.manifest.json"
    with pytest.raises(ValueError):
        archive_file_names(OUTCOMES)


def test_kind_sets_are_consistent():
    assert SESSION_KINDS == (*ALERT_KINDS, *FLOW_KINDS) and OUTCOMES not in SESSION_KINDS
    assert set(KIND_RELATIONS) == set(SESSION_KINDS) | {OUTCOMES}


def _flow_event(revision, minute=30, supersedes=None, ticker="O:SPY261016C00500000"):
    event_id = f"f0000000-0000-0000-0000-0000000000{revision:02d}"
    key = ("2026-10-05T14:%02d:00.000000Z" % minute, ticker, f"{7:020d}", f"{revision:020d}", event_id)
    return ArchiveRow("rt.option_flow_events", key, {
        "event_id": UUID(event_id), "event_time": datetime(2026, 10, 5, 14, minute, tzinfo=UTC),
        "revision": revision, "supersedes_event_id": supersedes, "price": Decimal("101.50000000"),
        "size": 25, "conditions": [1, 2], "opening_reasons": [], "quote_time": datetime(2026, 10, 5, 14, minute - 1, tzinfo=UTC),
        "created_at": datetime(2026, 10, 5, 14, minute, 1, tzinfo=UTC),
        "trade_treatment": "CANCELED" if revision > 1 else "INCLUDED",
    })


def _flow_cluster(revision, status, prints):
    cluster_id = "c0000000-0000-0000-0000-000000000001"
    key = ("2026-10-05T14:30:00.000000Z", cluster_id, f"{revision:020d}")
    return ArchiveRow("rt.option_flow_clusters", key, {
        "cluster_id": UUID(cluster_id), "revision": revision, "status": status, "print_count": prints,
        "total_premium": Decimal("0") if prints == 0 else Decimal("2537.50000000"),
        "first_event_time": datetime(2026, 10, 5, 14, 30, 5, tzinfo=UTC),
        "last_event_time": datetime(2026, 10, 5, 14, 30, 9, tzinfo=UTC),
        "created_at": datetime(2026, 10, 5, 14, 31, tzinfo=UTC),
    })


def test_option_flow_events_archive_every_revision_in_deterministic_order(tmp_path):
    first = _flow_event(1)
    cancel = _flow_event(2, supersedes=first.document["event_id"])
    later = _flow_event(10, supersedes=cancel.document["event_id"])
    other_print = _flow_event(1, minute=45, ticker="O:QQQ261016P00400000")
    rows = [later, other_print, cancel, first]
    built = build_archive(OPTION_FLOW_EVENTS, SESSION, rows)
    assert built.payload == build_archive(OPTION_FLOW_EVENTS, SESSION, list(reversed(rows))).payload
    lines = _lines(built)
    assert [(line["row"]["revision"], line["row"]["trade_treatment"]) for line in lines] == [
        (1, "INCLUDED"), (2, "CANCELED"), (10, "CANCELED"), (1, "INCLUDED")]  # revision 10 after 2
    assert lines[1]["row"]["supersedes_event_id"] == str(first.document["event_id"])
    assert lines[0]["row"]["price"] == "101.50000000" and lines[0]["row"]["conditions"] == [1, 2]
    assert built.relation_counts == {
        "rt.option_flow_events": 4,
        "rt.option_flow_event_profiles": 0,
    }
    assert built.clocks["market"]["columns"] == [
        "rt.option_flow_events.event_time", "rt.option_flow_events.quote_time"]
    assert built.clocks["market"]["min"] == "2026-10-05T14:29:00.000000Z"
    assert built.clocks["durable"]["max"] == "2026-10-05T14:45:01.000000Z"
    built, written = _written(tmp_path, built)
    verified = _verify(written, built)
    assert verified.row_count == 4 and written.data_path.name == "option_flow_events.jsonl.gz"


def test_option_flow_clusters_archive_every_revision_including_retracted(tmp_path):
    rows = [_flow_cluster(3, "RETRACTED", 0), _flow_cluster(1, "PROVISIONAL", 2), _flow_cluster(2, "FINAL", 2)]
    built = build_archive(OPTION_FLOW_CLUSTERS, SESSION, rows)
    assert [(line["row"]["revision"], line["row"]["status"]) for line in _lines(built)] == [
        (1, "PROVISIONAL"), (2, "FINAL"), (3, "RETRACTED")]
    assert _lines(built)[2]["row"]["print_count"] == 0
    assert built.relation_counts == {
        "rt.option_flow_clusters": 3,
        "rt.option_flow_cluster_members": 0,
    }
    built, written = _written(tmp_path, built)
    assert _verify(written, built).row_count == 3
    _expect(written, built, VerificationCode.SOURCE_COUNT_MISMATCH,
            source_counts={"rt.option_flow_clusters": 2})  # a revision stored after the archive is caught


def test_flow_files_only_accept_their_own_relation():
    with pytest.raises(ValueError, match="not part of"):
        build_archive(OPTION_FLOW_EVENTS, SESSION, [_flow_cluster(1, "FINAL", 1)])
    with pytest.raises(ValueError, match="not part of"):
        build_archive(OPTION_FLOW_CLUSTERS, SESSION, [_flow_event(1)])


# Verification ----------------------------------------------------------------------------


def test_verification_returns_the_manifest_identity_and_a_file_uri(tmp_path):
    built, written = _written(tmp_path)
    verified = _verify(written, built)
    assert verified.sha256 == built.sha256 and verified.row_count == 9
    assert verified.uri == written.data_path.resolve().as_uri() and verified.uri.startswith("file:")
    assert (verified.build_id, verified.policy_id) == ("build-1", "policy-1")


def _expect(written, built, code, **overrides):
    with pytest.raises(ArchiveVerificationError) as raised:
        _verify(written, built, **overrides)
    assert raised.value.code is code


def test_truncated_file_fails_by_hash_then_by_stream(tmp_path):
    built, written = _written(tmp_path)
    truncated = written.data_path.read_bytes()[:-12]
    written.data_path.write_bytes(truncated)
    _expect(written, built, VerificationCode.HASH_MISMATCH)
    _rewrite_manifest(written.manifest_path, sha256=hashlib.sha256(truncated).hexdigest())
    _expect(written, built, VerificationCode.UNREADABLE_CONTENT)


def test_flipped_byte_fails_the_hash_check(tmp_path):
    built, written = _written(tmp_path)
    data = bytearray(written.data_path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    written.data_path.write_bytes(bytes(data))
    _expect(written, built, VerificationCode.HASH_MISMATCH)


def test_row_count_mismatch_is_detected(tmp_path):
    built, written = _written(tmp_path)
    _rewrite_manifest(written.manifest_path, row_count=built.row_count + 1)
    _expect(written, built, VerificationCode.ROW_COUNT_MISMATCH)


def test_relation_count_mismatch_is_detected(tmp_path):
    built, written = _written(tmp_path)
    counts = dict(built.relation_counts, **{"rt.alert_current": 99})
    _rewrite_manifest(written.manifest_path, relation_counts=counts)
    _expect(written, built, VerificationCode.RELATION_COUNT_MISMATCH)


def test_source_count_mismatch_is_detected(tmp_path):
    built, written = _written(tmp_path)
    _expect(written, built, VerificationCode.SOURCE_COUNT_MISMATCH,
            source_counts=dict(built.relation_counts, **{"rt.alert_current": 4}))
    _expect(written, built, VerificationCode.SOURCE_COUNT_MISMATCH, source_counts={})


def test_content_hash_and_clock_mismatches_are_detected(tmp_path):
    built, written = _written(tmp_path)
    _rewrite_manifest(written.manifest_path, content_sha256="0" * 64)
    _expect(written, built, VerificationCode.CONTENT_HASH_MISMATCH)
    _rewrite_manifest(written.manifest_path, content_sha256=built.content_sha256, clocks={"market": None})
    _expect(written, built, VerificationCode.CLOCKS_MISMATCH)


def test_missing_invalid_and_foreign_manifests_are_detected(tmp_path):
    built, written = _written(tmp_path)
    original = written.manifest_path.read_text(encoding="ascii")
    _rewrite_manifest(written.manifest_path, schema_version="other-v9")
    _expect(written, built, VerificationCode.SCHEMA_UNSUPPORTED)
    _rewrite_manifest(written.manifest_path, schema_version=ARCHIVE_SCHEMA_VERSION, session_date="2026-10-06")
    _expect(written, built, VerificationCode.IDENTITY_MISMATCH)
    written.manifest_path.write_text(original, encoding="ascii")
    _expect(written, built, VerificationCode.IDENTITY_MISMATCH, session_date=date(2026, 10, 6))
    _expect(written, built, VerificationCode.IDENTITY_MISMATCH, kind=DETECTOR_EVALUATIONS)
    written.manifest_path.write_text("{", encoding="ascii")
    _expect(written, built, VerificationCode.MANIFEST_INVALID)
    written.manifest_path.write_text('{"row_count": true}', encoding="ascii")
    _expect(written, built, VerificationCode.MANIFEST_INVALID)
    written.manifest_path.unlink()
    _expect(written, built, VerificationCode.MANIFEST_MISSING)


def test_missing_data_file_is_detected(tmp_path):
    built, written = _written(tmp_path)
    written.data_path.unlink()
    _expect(written, built, VerificationCode.FILE_MISSING)


def test_a_line_that_is_not_a_relation_row_is_detected(tmp_path):
    built, written = _written(tmp_path)
    for body in (b"not json\n", b'{"relation": "rt.unknown", "row": {}}\n', b'{"relation": "rt.alert_current"}\n'):
        payload = gzip.compress(body, mtime=0)
        written.data_path.write_bytes(payload)
        _rewrite_manifest(
            written.manifest_path, sha256=hashlib.sha256(payload).hexdigest(),
            content_sha256=hashlib.sha256(body).hexdigest(), uncompressed_bytes=len(body))
        _expect(written, built, VerificationCode.LINE_INVALID)


# Drop decision -------------------------------------------------------------------------------

WEEK = PartitionRange("rt.alert_current", date(2026, 9, 14), date(2026, 9, 21))
WEEK_SESSIONS = [date(2026, 9, 14 + offset) for offset in range(5)]
NEXT_SESSION = date(2026, 9, 21)


def _records(sessions=WEEK_SESSIONS, kinds=ALERT_KINDS, verified=True):
    return [
        ArchiveRecord(session, kind, f"file:///a/{session}/{kind}", "a" * 64, 1, ARCHIVE_SCHEMA_VERSION,
                      "build-1", VERIFIED_AT if verified else None)
        for session in sessions for kind in kinds
    ]


def _decide(records=None, pins=(), sessions=WEEK_SESSIONS, window_start=date(2026, 10, 12), partition=WEEK,
            required_kinds=ALERT_KINDS):
    return decide_partition_drop(partition, sessions, _records() if records is None else records, pins,
                                 required_kinds=required_kinds, window_start=window_start)


def _codes(decision):
    return [(reason.code, reason.session_date, reason.kind) for reason in decision.reasons]


def test_all_sessions_verified_without_pins_drops():
    decision = _decide()
    assert decision.drop and decision.action == DROP
    assert [reason.code for reason in decision.reasons] == [REASON_ARCHIVES_VERIFIED]


@pytest.mark.parametrize("kind", ALERT_KINDS)
def test_a_missing_required_kind_keeps_the_partition(kind):
    records = [record for record in _records()
               if not (record.session_date == date(2026, 9, 16) and record.kind == kind)]
    decision = _decide(records)
    assert decision.action == KEEP
    assert _codes(decision) == [(REASON_ARCHIVE_MISSING, date(2026, 9, 16), kind)]


def test_a_whole_missing_session_lists_every_kind():
    records = [record for record in _records() if record.session_date != date(2026, 9, 18)]
    decision = _decide(records)
    assert [reason.kind for reason in decision.reasons] == sorted(ALERT_KINDS)
    assert {reason.code for reason in decision.reasons} == {REASON_ARCHIVE_MISSING}


# The required kinds come from the relation's partition policy, not from a fixed list.
@pytest.mark.parametrize(("required", "present", "drop"), [
    ((OPTION_FLOW_EVENTS,), (OPTION_FLOW_EVENTS,), True),
    ((OPTION_FLOW_EVENTS,), (OPTION_FLOW_CLUSTERS,), False),     # another kind never stands in
    ((OPTION_FLOW_EVENTS,), ALERT_KINDS, False),
    ((OPTION_FLOW_CLUSTERS,), (OPTION_FLOW_CLUSTERS,), True),
    ((OPTION_FLOW_CLUSTERS,), (OPTION_FLOW_EVENTS,), False),
    (FLOW_KINDS, FLOW_KINDS, True),
    (FLOW_KINDS, (OPTION_FLOW_EVENTS,), False),                  # ANY required kind missing keeps it
    (FLOW_KINDS, (OPTION_FLOW_CLUSTERS,), False),
    ((*ALERT_KINDS, OPTION_FLOW_EVENTS), (*ALERT_KINDS, OPTION_FLOW_EVENTS), True),
    ((*ALERT_KINDS, OPTION_FLOW_EVENTS), ALERT_KINDS, False),
])
def test_required_kinds_follow_the_policy(required, present, drop):
    decision = _decide(_records(kinds=present), required_kinds=required)
    assert decision.drop is drop
    if not drop:
        missing = sorted(set(required) - set(present))
        assert {reason.kind for reason in decision.reasons} == set(missing)
        assert {reason.code for reason in decision.reasons} == {REASON_ARCHIVE_MISSING}


def test_a_required_kind_unverified_in_one_session_keeps_the_partition():
    records = _records(kinds=FLOW_KINDS)
    records[-1] = ArchiveRecord(WEEK_SESSIONS[-1], OPTION_FLOW_CLUSTERS, "file:///x", "b" * 64, 1,
                                ARCHIVE_SCHEMA_VERSION, "b", None)
    decision = _decide(records, required_kinds=FLOW_KINDS)
    assert _codes(decision) == [(REASON_ARCHIVE_UNVERIFIED, WEEK_SESSIONS[-1], OPTION_FLOW_CLUSTERS)]


def test_a_decision_needs_at_least_one_required_kind():
    with pytest.raises(ValueError, match="required archive kind"):
        _decide(required_kinds=())
    with pytest.raises(TypeError):
        decide_partition_drop(WEEK, WEEK_SESSIONS, _records(), [])  # there is no default kind list


def test_an_unverified_record_does_not_count():
    records = _records(WEEK_SESSIONS[:-1]) + _records(WEEK_SESSIONS[-1:], verified=False)
    decision = _decide(records)
    assert decision.action == KEEP
    assert {reason.code for reason in decision.reasons} == {REASON_ARCHIVE_UNVERIFIED}


def test_an_outcomes_record_cannot_stand_in_for_a_required_kind():
    extra = ArchiveRecord(date(2026, 9, 14), OUTCOMES, "file:///x", "b" * 64, 1, ARCHIVE_SCHEMA_VERSION, "b",
                          VERIFIED_AT, horizon_close_date=date(2026, 9, 15))
    records = [r for r in _records() if not (r.session_date == date(2026, 9, 14) and r.kind == STOCK_ALERTS)]
    assert _decide(records + [extra]).action == KEEP


@pytest.mark.parametrize("reason", [PIN_OPEN_HORIZON, PIN_OUTCOME_UNVERIFIED])
def test_an_open_pin_keeps_a_fully_archived_partition(reason):
    decision = _decide(pins=[Pin(date(2026, 9, 17), reason, "alert-1")])
    assert decision.action == KEEP
    [only] = decision.reasons
    assert (only.code, only.session_date, only.detail) == (REASON_OPEN_PIN, date(2026, 9, 17), f"{reason}:alert-1")
    assert only.as_dict() == {"code": REASON_OPEN_PIN, "session_date": "2026-09-17", "kind": None,
                              "detail": f"{reason}:alert-1"}


def test_boundary_sessions_belong_to_exactly_one_partition():
    assert WEEK.contains(WEEK.lower) and not WEEK.contains(WEEK.upper)
    # An archive and a pin of the following partition's first session do not matter here...
    assert _decide(pins=[Pin(NEXT_SESSION, PIN_OPEN_HORIZON, "later")]).drop
    assert _decide(records=_records(WEEK_SESSIONS + [NEXT_SESSION])).drop
    # ...but the first and last sessions of this partition each count fully.
    assert _decide(pins=[Pin(WEEK.lower, PIN_OPEN_HORIZON, "first")]).action == KEEP
    assert _decide(pins=[Pin(date(2026, 9, 20), PIN_OPEN_HORIZON, "last")]).action == KEEP
    missing_last = [r for r in _records() if r.session_date != WEEK_SESSIONS[-1]]
    assert _decide(missing_last).action == KEEP
    missing_next_only = _records(WEEK_SESSIONS)
    assert _decide(missing_next_only).drop


def test_sessions_outside_the_partition_are_a_caller_error():
    with pytest.raises(ValueError, match="outside"):
        _decide(sessions=[*WEEK_SESSIONS, NEXT_SESSION])


def test_a_partition_with_no_sessions_fails_closed():
    decision = _decide(sessions=[], records=[])
    assert decision.action == KEEP and [reason.code for reason in decision.reasons] == [REASON_NO_SESSIONS]


def test_online_window_boundary():
    assert _decide(window_start=WEEK.upper).drop  # upper bound is exclusive: every session is older
    kept = _decide(window_start=date(2026, 9, 20))
    assert kept.action == KEEP and kept.reasons[0].code == REASON_WITHIN_ONLINE_WINDOW
    assert decide_partition_drop(WEEK, WEEK_SESSIONS, _records(), [], required_kinds=ALERT_KINDS).drop  # no window


def test_reasons_are_aggregated_in_a_stable_order():
    records = [r for r in _records() if r.session_date != date(2026, 9, 15)]
    pins = [Pin(date(2026, 9, 18), PIN_OPEN_HORIZON, "z"), Pin(date(2026, 9, 14), PIN_OPEN_HORIZON, "a")]
    first = _decide(records, pins, window_start=date(2026, 9, 20))
    second = _decide(list(reversed(records)), list(reversed(pins)), window_start=date(2026, 9, 20))
    assert first == second
    assert [reason.code for reason in first.reasons] == (
        [REASON_ARCHIVE_MISSING] * 3 + [REASON_OPEN_PIN] * 2 + [REASON_WITHIN_ONLINE_WINDOW])


def test_partition_range_must_be_nonempty():
    with pytest.raises(ValueError):
        PartitionRange("rt.alert_current", date(2026, 9, 14), date(2026, 9, 14))
