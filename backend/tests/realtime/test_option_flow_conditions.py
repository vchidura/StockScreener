from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path

import pytest

from option_flow_support import (
    CANCEL,
    NON_VOLUME,
    PAYLOAD,
    REGULAR,
    REVIEW,
    UNKNOWN_CODE,
    UNREVIEWED,
    config,
    semantics,
)
from options.data.trade_classification import TRADE_SEMANTICS_VERSION
from realtime.option_flow import conditions as conditions_module
from realtime.option_flow.conditions import (
    PROVENANCE_LIVE_CAPTURE,
    PROVENANCE_SYNTHETIC,
    SEMANTICS_VERSION,
    ConditionPinError,
    Treatment,
    build_semantics,
    load_semantics,
    parse_payload,
    parse_review,
)

PACKAGE = Path(conditions_module.__file__).parent


def _documents():
    return (json.loads(PAYLOAD.read_text(encoding="utf-8")),
            json.loads(REVIEW.read_text(encoding="utf-8")))


def test_fixture_is_labelled_synthetic_and_never_presented_as_live():
    payload, review = _documents()
    assert payload["provenance"] == PROVENANCE_SYNTHETIC == review["provenance"]
    assert "NOT_LIVE_CAPTURED" in payload["provenance"]
    assert all(row["name"].startswith("SYNTHETIC") for row in payload["results"])
    pinned = semantics()
    assert not pinned.is_live_capture
    assert pinned.pin.provenance == PROVENANCE_SYNTHETIC


def test_semantics_version_is_new_and_v1_is_untouched():
    assert SEMANTICS_VERSION == "massive_options_conditions_v2"
    assert SEMANTICS_VERSION != TRADE_SEMANTICS_VERSION == "massive_options_conditions_v1"
    assert semantics().version == SEMANTICS_VERSION


def test_pin_is_the_canonical_payload_hash():
    payload, review = _documents()
    pinned = semantics()
    assert pinned.pin.payload_sha256 == review["reviewed_payload_sha256"]
    assert pinned.pin.payload_sha256 == parse_payload(payload).payload_sha256
    assert len(pinned.semantics_sha256) == 64


def test_envelope_changes_do_not_change_the_hash_but_content_changes_do():
    payload, review = _documents()
    baseline = parse_payload(payload).payload_sha256
    payload["request_id"] = "transient"
    payload["count"] = 999
    assert parse_payload(payload).payload_sha256 == baseline
    payload["results"][0]["name"] = "SYNTHETIC regular sale, edited"
    changed = parse_payload(payload)
    assert changed.payload_sha256 != baseline
    with pytest.raises(ConditionPinError) as raised:
        build_semantics(changed, parse_review(review))
    assert raised.value.reason == "PAYLOAD_HASH_MISMATCH"


def test_a_different_payload_cannot_be_accepted_even_with_its_own_review_hash():
    payload, review = _documents()
    payload["results"][1]["update_rules"]["consolidated"]["updates_volume"] = False
    edited = parse_payload(payload)
    with pytest.raises(ConditionPinError) as raised:
        build_semantics(edited, parse_review(review), expected_payload_sha256=parse_payload(_documents()[0]).payload_sha256)
    assert raised.value.reason == "PAYLOAD_HASH_MISMATCH"


def test_explicit_pin_must_match_the_reviewed_hash():
    with pytest.raises(ConditionPinError) as raised:
        load_semantics(PAYLOAD, REVIEW, expected_payload_sha256="0" * 64)
    assert raised.value.reason == "PAYLOAD_HASH_MISMATCH"


def test_provenance_mismatch_between_payload_and_review_is_rejected():
    payload, review = _documents()
    review["provenance"] = PROVENANCE_LIVE_CAPTURE
    with pytest.raises(ConditionPinError) as raised:
        build_semantics(parse_payload(payload), parse_review(review))
    assert raised.value.reason == "PROVENANCE_MISMATCH"


def test_unknown_code_fails_closed_with_a_machine_readable_reason():
    verdict = semantics().classify((UNKNOWN_CODE,))
    assert verdict.treatment is Treatment.UNKNOWN
    assert not verdict.known and not verdict.volume_eligible
    assert verdict.reasons == (f"UNKNOWN_TRADE_CONDITION_{UNKNOWN_CODE}",)


def test_provider_code_without_a_review_fails_closed_and_is_listed():
    pinned = semantics()
    assert UNREVIEWED in pinned.unreviewed_ids
    verdict = pinned.classify((UNREVIEWED,))
    assert verdict.treatment is Treatment.UNKNOWN
    assert verdict.reasons == (f"UNKNOWN_TRADE_CONDITION_{UNREVIEWED}",)


def test_one_unknown_code_poisons_an_otherwise_included_print():
    verdict = semantics().classify((REGULAR, UNKNOWN_CODE))
    assert verdict.treatment is Treatment.UNKNOWN and not verdict.volume_eligible


def test_missing_conditions_fail_closed():
    verdict = semantics().classify(())
    assert verdict.treatment is Treatment.UNKNOWN
    assert verdict.reasons == ("TRADE_CONDITION_MISSING",)


def test_cancel_wins_over_included_and_non_volume_is_excluded():
    pinned = semantics()
    canceled = pinned.classify((REGULAR, CANCEL))
    assert canceled.treatment is Treatment.CANCELED and canceled.is_cancel and not canceled.volume_eligible
    excluded = pinned.classify((REGULAR, NON_VOLUME))
    assert excluded.treatment is Treatment.EXCLUDED and not excluded.volume_eligible


def test_flags_are_combined_across_codes():
    verdict = semantics().classify((9006, 9005, 9003, 9004, 9008))
    assert verdict.treatment is Treatment.INCLUDED
    assert (verdict.is_multi_leg, verdict.is_cross, verdict.is_sweep, verdict.is_floor, verdict.is_late) == (
        True, True, True, True, True)


def test_included_code_must_agree_with_the_provider_volume_rule():
    payload, review = _documents()
    payload["results"][0]["update_rules"]["consolidated"]["updates_volume"] = False
    edited = parse_payload(payload)
    review["reviewed_payload_sha256"] = edited.payload_sha256
    with pytest.raises(ConditionPinError) as raised:
        build_semantics(edited, parse_review(review))
    assert raised.value.reason == "PROVIDER_VOLUME_RULE_CONFLICT"


def test_review_of_a_code_absent_from_the_payload_is_rejected():
    payload, review = _documents()
    review["codes"].append({"condition_id": 4242, "treatment": "INCLUDED"})
    with pytest.raises(ConditionPinError) as raised:
        build_semantics(parse_payload(payload), parse_review(review))
    assert raised.value.reason == "REVIEW_CODE_NOT_IN_PAYLOAD"


def test_reference_rows_follow_the_database_rules():
    pinned = semantics()
    codes = pinned.trade_condition_rows()
    reviewed = pinned.semantics_rows()
    assert {row.provider for row in codes} == {"massive_synthetic_fixture"}
    assert {row.payload_sha256 for row in codes} == {pinned.pin.payload_sha256}
    assert {row.source_payload_sha256 for row in reviewed} == {pinned.pin.payload_sha256}
    assert {row.semantics_version for row in reviewed} == {SEMANTICS_VERSION}
    for row in reviewed:
        assert row.treatment in ("INCLUDED", "EXCLUDED", "CANCELED")
        assert row.treatment != "CANCELED" or row.is_cancel
        assert row.treatment != "INCLUDED" or (row.volume_eligible and not row.is_cancel)
    assert len(codes) == 13 and len(reviewed) == len(pinned.reviewed) == 12


def test_engine_refuses_a_synthetic_pin_unless_explicitly_allowed():
    with pytest.raises(ValueError, match="SYNTHETIC_SEMANTICS_NOT_ALLOWED"):
        replace(config(), allow_synthetic_semantics=False)


def test_no_certainty_claim_appears_anywhere_in_the_package_or_fixtures():
    needle = "guar" + "anteed"
    offenders = [path.name for path in PACKAGE.glob("*.py") if needle in path.read_text(encoding="utf-8").lower()]
    assert offenders == []
    assert not any(re.search(needle, path.read_text(encoding="utf-8"), re.IGNORECASE) for path in (PAYLOAD, REVIEW))
