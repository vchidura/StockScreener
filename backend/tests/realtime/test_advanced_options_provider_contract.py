from dataclasses import replace
import json

import pytest

from realtime.advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    DEFAULT_OPTIONS_EXCHANGE_REFERENCE_PATH,
    PROVENANCE_SYNTHETIC,
    parse_exchange_reference,
)
from realtime.option_flow.messages import (
    QUOTE_PROVIDER_FIELDS,
    TRADE_PROVIDER_FIELDS,
)


def _exchange_document():
    return {
        "provenance": PROVENANCE_SYNTHETIC,
        "fetched_at": "2026-10-18T00:00:00Z",
        "request": {"asset_class": "options", "locale": "us"},
        "results": [
            {
                "id": 302,
                "acronym": "SYNX",
                "asset_class": "options",
                "locale": "us",
                "mic": "SYNX",
                "name": "SYNTHETIC OPTIONS EXCHANGE",
                "operating_mic": "SYNX",
                "participant_id": "S",
                "type": "exchange",
                "url": "https://example.invalid/synthetic",
            },
            {
                "id": 301,
                "asset_class": "options",
                "locale": "us",
                "name": "SYNTHETIC OPTIONS EXCHANGE TWO",
                "type": "exchange",
            },
        ],
    }


def test_documented_provider_contract_pins_exact_fields_and_endpoints():
    contract = MASSIVE_OPTIONS_PROVIDER_CONTRACT

    assert contract.trade_fields == ("sym", "x", "p", "s", "c", "t", "q")
    assert contract.quote_fields == (
        "sym",
        "bx",
        "ax",
        "bp",
        "ap",
        "bs",
        "as",
        "t",
        "q",
    )
    assert contract.condition_endpoint == "/v3/reference/conditions"
    assert contract.exchange_endpoint == "/v3/reference/exchanges"
    assert contract.forbidden_dependencies == ("m",)
    assert TRADE_PROVIDER_FIELDS == contract.trade_fields
    assert QUOTE_PROVIDER_FIELDS == contract.quote_fields
    assert len(contract.sha256) == 64


def test_recognized_payload_ignores_unreviewed_fields_without_dependency():
    payload = {
        "ev": "T",
        "sym": "O:SPY261019C00650000",
        "x": 302,
        "p": 2.0,
        "s": 50,
        "c": [1],
        "t": 1,
        "q": 2,
        "m": 999,
    }

    recognized = MASSIVE_OPTIONS_PROVIDER_CONTRACT.recognized_payload(payload)

    assert set(recognized) == {"ev", *MASSIVE_OPTIONS_PROVIDER_CONTRACT.trade_fields}
    assert "m" not in recognized
    with pytest.raises(ValueError, match="missing pinned fields"):
        MASSIVE_OPTIONS_PROVIDER_CONTRACT.recognized_payload(
            {key: value for key, value in payload.items() if key != "q"}
        )


def test_exchange_reference_retains_provider_ids_and_full_mapping_identity():
    reference = parse_exchange_reference(_exchange_document())

    assert not reference.is_live_capture
    assert [row.exchange_id for row in reference.exchanges] == [301, 302]
    assert reference.entry_for(302).participant_id == "S"
    assert reference.entry_for(302).mic == "SYNX"
    assert len(reference.payload_sha256) == 64
    assert len(reference.sha256) == 64
    with pytest.raises(ValueError, match="outside"):
        reference.entry_for(999)


def test_live_exchange_capture_is_pinned_but_not_queried_at_runtime():
    document = json.loads(
        DEFAULT_OPTIONS_EXCHANGE_REFERENCE_PATH.read_text(
            encoding="utf-8"
        )
    )
    reference = parse_exchange_reference(document)

    assert reference.is_live_capture
    assert len(reference.exchanges) == 21
    assert reference.payload_sha256 == (
        "f49d300a4795448bdc12ae09c2d578c611933c06a78d3cb84a5e67044ca032b3"
    )
    assert reference.sha256 == (
        "ceb02f24d3b5a79c54e604eefe6070b6e95239f5f14028cc8fd6ee0716191cc5"
    )
    assert reference.entry_for(314).exchange_type == "SIP"


def test_exchange_mapping_or_payload_change_changes_identity():
    baseline = parse_exchange_reference(_exchange_document())
    changed_document = _exchange_document()
    changed_document["results"][0]["participant_id"] = "T"
    changed = parse_exchange_reference(changed_document)

    assert changed.payload_sha256 != baseline.payload_sha256
    assert changed.sha256 != baseline.sha256
    assert replace(
        baseline,
        provenance="LIVE_CAPTURED",
    ).sha256 != baseline.sha256


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: value["request"].update(locale="global"),
        lambda value: value["results"][0].update(unexpected=True),
        lambda value: value["results"][0].update(asset_class="stocks"),
        lambda value: value["results"].append(dict(value["results"][0])),
    ),
)
def test_exchange_reference_scope_shape_and_ids_fail_closed(mutation):
    document = _exchange_document()
    mutation(document)

    with pytest.raises(ValueError):
        parse_exchange_reference(document)
