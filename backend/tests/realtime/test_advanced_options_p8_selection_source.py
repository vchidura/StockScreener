from __future__ import annotations

import json
import hashlib
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from options.data.polygon_http import PolygonHttpResponse
from realtime.advanced_options_p8_selection_source import (
    MassiveAdvancedOptionsExactContractSource,
    MassiveAdvancedOptionsSelectionSource,
    SEALED_CAPTURE_EXACT_FOUR_PROFILE,
    SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
    P8SelectionNoUsableContracts,
    P8SelectionProviderAttempt,
    _candidate_from_snapshot,
    _snapshot_rejection_reason,
    p8_activity_selection_capture_from_sealed,
)
from realtime.advanced_options_p8_production import (
    _cycle_cutoff,
    _open_interest_sha256,
    _p8_manifest_policy,
)
from realtime.advanced_options_flow_policy import (
    load_advanced_options_flow_policy,
)
from realtime.advanced_options_p7_selection import (
    P7TransportSelectionPolicy,
)
from realtime.contracts import canonical_sha256
from scripts import prepare_advanced_options_v3_pre_session
from scripts import replay_advanced_options_v3_capture_local


UTC = timezone.utc
SESSION = date(2026, 10, 21)
REVIEWED = datetime(2026, 10, 21, 13, 29, tzinfo=UTC)
MARKET_NS = int(
    datetime(2026, 10, 21, 13, 28, 30, tzinfo=UTC).timestamp()
    * 1_000_000_000
)


def _contract(underlyer, ticker, kind, strike, spot, volume=500):
    return {
        "details": {
            "ticker": ticker,
            "contract_type": kind,
            "expiration_date": SESSION.isoformat(),
            "strike_price": strike,
        },
        "last_quote": {
            "bid": 1,
            "ask": 1.1,
            "last_updated": MARKET_NS,
        },
        "day": {"volume": volume},
        "open_interest": 1_000,
        "underlying_asset": {
            "ticker": underlyer,
            "price": spot,
            "last_updated": MARKET_NS,
            "timeframe": "REAL-TIME",
        },
    }


class _Transport:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        payload = self.payloads.pop(0)
        return PolygonHttpResponse(
            200,
            json.dumps(payload).encode("utf-8"),
        )


class _Spots:
    def load_spots(self, *, underlyings, available_before):
        assert underlyings == ("QQQ", "SPY")
        assert available_before == REVIEWED
        return {
            "QQQ": Decimal("600"),
            "SPY": Decimal("670"),
        }


def _payload(underlyer, rows, **changes):
    value = {
        "status": "OK",
        "request_id": f"{underlyer}-request",
        "results": rows,
    }
    value.update(changes)
    return value


def test_advanced_source_uses_exact_two_filtered_requests_without_pagination():
    transport = _Transport([
        _payload("QQQ", [
            _contract(
                "QQQ",
                "O:QQQ261021C00600000",
                "call",
                600,
                600,
            ),
        ]),
        _payload("SPY", [
            _contract(
                "SPY",
                "O:SPY261021P00670000",
                "put",
                670,
                670,
            ),
        ]),
    ])
    capture = MassiveAdvancedOptionsSelectionSource(
        transport,
        _Spots(),
    ).capture(reviewed_at=REVIEWED, session_date=SESSION)

    assert capture.request_count == capture.pages == 2
    assert capture.pagination_requests == 0
    assert capture.selection.contracts == (
        "O:QQQ261021C00600000",
        "O:SPY261021P00670000",
    )
    assert capture.evidence["request_count"] == 2
    assert capture.evidence["source_snapshot_at_utc"] == (
        "2026-10-21T13:28:30+00:00"
    )
    assert len(transport.calls) == 2
    for _, params in transport.calls:
        assert params["expiration_date.gte"] == "2026-10-21"
        assert params["expiration_date.lte"] == "2026-10-21"
        assert params["limit"] == "250"
    assert transport.calls[0][1]["strike_price.gte"] == "570.00"
    assert transport.calls[0][1]["strike_price.lte"] == "630.00"
    assert transport.calls[1][1]["strike_price.gte"] == "636.50"
    assert transport.calls[1][1]["strike_price.lte"] == "703.50"


def test_advanced_source_can_retain_profile_scoped_c6_candidate_pool():
    transport = _Transport([
        _payload("QQQ", [
            _contract(
                "QQQ",
                "O:QQQ261021C00600000",
                "call",
                600,
                600,
            ),
            _contract(
                "QQQ",
                "O:QQQ261021P00595000",
                "put",
                595,
                600,
            ),
        ]),
        _payload("SPY", [
            _contract(
                "SPY",
                "O:SPY261021C00670000",
                "call",
                670,
                670,
            ),
            _contract(
                "SPY",
                "O:SPY261021P00665000",
                "put",
                665,
                670,
            ),
        ]),
    ])
    capture = MassiveAdvancedOptionsSelectionSource(
        transport,
        _Spots(),
        policy=P7TransportSelectionPolicy(
            version="c6-candidate-pool-test-v1",
            underlyings=("QQQ", "SPY"),
            minimum_dte=0,
            maximum_dte=21,
            maximum_moneyness_fraction=Decimal("0.10"),
        ),
        retain_candidate_pool=True,
        query_minimum_dte=0,
        query_maximum_dte=21,
        query_maximum_moneyness_fraction=Decimal("0.10"),
    ).capture(reviewed_at=REVIEWED, session_date=SESSION)

    assert capture.selection.contracts == (
        "O:QQQ261021C00600000",
        "O:QQQ261021P00595000",
        "O:SPY261021C00670000",
        "O:SPY261021P00665000",
    )
    assert capture.evidence["schema"] == (
        "advanced-options-c6-candidate-pool-v1"
    )
    assert capture.evidence["candidate_count"] == 4
    assert capture.evidence["query"] == {
        "minimum_dte": 0,
        "maximum_dte": 21,
        "maximum_moneyness_fraction": "0.10",
    }
    assert transport.calls[0][1]["expiration_date.lte"] == "2026-11-11"
    assert transport.calls[0][1]["strike_price.gte"] == "540.00"
    assert transport.calls[0][1]["strike_price.lte"] == "660.00"


def test_advanced_source_enforces_granted_response_and_total_bytes():
    payloads = [
        _payload("QQQ", [
            _contract(
                "QQQ",
                "O:QQQ261021C00600000",
                "call",
                600,
                600,
            ),
        ]),
        _payload("SPY", [
            _contract(
                "SPY",
                "O:SPY261021P00670000",
                "put",
                670,
                670,
            ),
        ]),
    ]
    sizes = [
        len(json.dumps(payload).encode("utf-8"))
        for payload in payloads
    ]
    with pytest.raises(ValueError, match="response decoded-byte"):
        MassiveAdvancedOptionsSelectionSource(
            _Transport(payloads),
            _Spots(),
            maximum_response_decoded_bytes=1,
            maximum_total_decoded_bytes=sum(sizes),
        ).capture(reviewed_at=REVIEWED, session_date=SESSION)

    with pytest.raises(ValueError, match="decoded-byte ceiling"):
        MassiveAdvancedOptionsSelectionSource(
            _Transport(payloads),
            _Spots(),
            maximum_response_decoded_bytes=max(sizes),
            maximum_total_decoded_bytes=sum(sizes) - 1,
        ).capture(reviewed_at=REVIEWED, session_date=SESSION)


def test_pre_session_cli_accepts_stricter_granted_byte_ceilings(
    tmp_path,
):
    args = prepare_advanced_options_v3_pre_session.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--source-cohort-id",
        "11111111-2222-3333-4444-555555555555",
        "--session-date",
        "2026-10-28",
        "--authorized-until",
        "2026-10-28T20:05:00+00:00",
        "--reference-valid-until",
        "2026-10-28T20:05:00+00:00",
        "--maximum-provider-requests",
        "2",
        "--maximum-response-decoded-bytes",
        "1048576",
        "--maximum-decoded-bytes",
        "18874386",
        "--maximum-moneyness-fraction",
        "0.05",
        "--authorization-reference",
        "grant-test",
        "--enabled-strategy",
        "DIRECTIONAL_DEBIT_SPREAD",
        "--capture-path",
        str(tmp_path / "capture.json"),
        "--report-path",
        str(tmp_path / "report.json"),
    ])

    assert args.maximum_response_decoded_bytes == 1_048_576
    assert args.maximum_decoded_bytes == 18_874_386
    assert args.enabled_strategies == (
        "DIRECTIONAL_DEBIT_SPREAD",
    )


def test_pre_session_cli_accepts_capture_only_mode(tmp_path):
    args = prepare_advanced_options_v3_pre_session.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--source-cohort-id",
        "11111111-1111-1111-1111-111111111111",
        "--session-date",
        "2026-10-30",
        "--authorized-until",
        "2026-10-30T13:29:00+00:00",
        "--reference-valid-until",
        "2026-10-30T20:05:00+00:00",
        "--maximum-provider-requests",
        "2",
        "--maximum-response-decoded-bytes",
        "50000000",
        "--maximum-decoded-bytes",
        "100000000",
        "--maximum-moneyness-fraction",
        "0.05",
        "--authorization-reference",
        "local-capture-test",
        "--capture-path",
        str(tmp_path / "capture.json"),
        "--report-path",
        str(tmp_path / "report.json"),
        "--capture-only",
        "--execute",
    ])

    assert args.capture_only is True


def test_capture_only_skips_reference_fact_transaction():
    class _Provider:
        def transaction(self, *_args, **_kwargs):
            raise AssertionError("capture-only must not open a transaction")

    assert prepare_advanced_options_v3_pre_session._persist_facts(
        _Provider(),
        (object(),),
        capture_only=True,
    ) == 0


def test_chain_candidate_skips_unquoted_contract():
    row = _contract(
        "QQQ",
        "O:QQQ261030C00600000",
        "call",
        600,
        600,
    )
    row["last_quote"]["ask"] = 0

    assert _candidate_from_snapshot(
        row,
        session_date=date(2026, 10, 30),
    ) is None


def test_activity_candidate_keeps_missing_oi_explicit():
    raw = _contract(
        "QQQ",
        "O:QQQ261021C00600000",
        "call",
        600,
        600,
    )
    raw.pop("open_interest")
    candidate = _candidate_from_snapshot(
        raw,
        session_date=SESSION,
    )
    assert candidate is not None

    activity = (
        prepare_advanced_options_v3_pre_session._activity_candidate(
            candidate,
            raw,
            session_date=SESSION,
            observed_at=REVIEWED,
            flow_policy=load_advanced_options_flow_policy(),
        )
    )

    assert activity.prior_open_interest is None
    assert "MISSING_PRIOR_OPEN_INTEREST" in activity.missing_inputs


def test_advanced_source_follows_bounded_pagination():
    paginated = _Transport([
        _payload(
            "QQQ",
            [_contract(
                "QQQ",
                "O:QQQ261021C00600000",
                "call",
                600,
                600,
            )],
            next_url=(
                "https://api.massive.com/v3/snapshot/options/"
                "QQQ?cursor=page-2"
            ),
        ),
        _payload("QQQ-page-2", [
            _contract(
                "QQQ",
                "O:QQQ261021P00600000",
                "put",
                600,
                600,
            ),
        ]),
        _payload("SPY", [
            _contract(
                "SPY",
                "O:SPY261021P00670000",
                "put",
                670,
                670,
            ),
        ]),
    ])
    capture = MassiveAdvancedOptionsSelectionSource(
        paginated,
        _Spots(),
        maximum_provider_requests=3,
    ).capture(reviewed_at=REVIEWED, session_date=SESSION)

    assert capture.request_count == capture.pages == 3
    assert capture.pagination_requests == 1
    assert paginated.calls[1] == (
        (
            "https://api.massive.com/v3/snapshot/options/"
            "QQQ?cursor=page-2"
        ),
        {},
    )


def test_advanced_source_rejects_pagination_over_request_ceiling():
    paginated = _Transport([
        _payload(
            "QQQ",
            [_contract(
                "QQQ",
                "O:QQQ261021C00600000",
                "call",
                600,
                600,
            )],
            next_url=(
                "https://api.massive.com/v3/snapshot/options/"
                "QQQ?cursor=page-2"
            ),
        ),
        _payload("QQQ-page-2", [
            _contract(
                "QQQ",
                "O:QQQ261021P00600000",
                "put",
                600,
                600,
            ),
        ]),
    ])
    with pytest.raises(ValueError, match="budgets"):
        MassiveAdvancedOptionsSelectionSource(
            paginated,
            _Spots(),
            maximum_provider_requests=2,
        ).capture(reviewed_at=REVIEWED, session_date=SESSION)


def test_advanced_source_enforces_pagination_deadline_and_scope():
    source = MassiveAdvancedOptionsSelectionSource(
        _Transport([]),
        _Spots(),
        maximum_provider_requests=3,
        deadline=REVIEWED,
        clock=lambda: REVIEWED + timedelta(seconds=1),
    )
    with pytest.raises(TimeoutError, match="deadline"):
        source.capture(reviewed_at=REVIEWED, session_date=SESSION)

    unapproved = _Transport([
        _payload(
            "QQQ",
            [_contract(
                "QQQ",
                "O:QQQ261021C00600000",
                "call",
                600,
                600,
            )],
            next_url="https://example.com/v3/snapshot/options/QQQ",
        ),
    ])
    with pytest.raises(ValueError, match="next_url"):
        MassiveAdvancedOptionsSelectionSource(
            unapproved,
            _Spots(),
            maximum_provider_requests=3,
        ).capture(reviewed_at=REVIEWED, session_date=SESSION)


def test_advanced_source_rejects_incomplete_activity():
    blocked = _Transport([
        _payload("QQQ", [
            _contract(
                "QQQ",
                "O:QQQ261021C00600000",
                "call",
                600,
                600,
                volume=1,
            ),
        ]),
        _payload("SPY", [
            _contract(
                "SPY",
                "O:SPY261021P00670000",
                "put",
                670,
                670,
                volume=1,
            ),
        ]),
    ])
    with pytest.raises(RuntimeError, match="blocked"):
        MassiveAdvancedOptionsSelectionSource(
            blocked,
            _Spots(),
        ).capture(reviewed_at=REVIEWED, session_date=SESSION)


def test_advanced_source_uses_capture_completion_for_quote_time():
    captured_at = REVIEWED.replace(second=1)
    row = _contract(
        "QQQ",
        "O:QQQ261021C00600000",
        "call",
        600,
        600,
    )
    row["last_quote"]["last_updated"] = int(
        captured_at.timestamp() * 1_000_000_000
    )
    spy = _contract(
        "SPY",
        "O:SPY261021P00670000",
        "put",
        670,
        670,
    )
    spy["last_quote"]["last_updated"] = int(
        captured_at.timestamp() * 1_000_000_000
    )

    capture = MassiveAdvancedOptionsSelectionSource(
        _Transport([
            _payload("QQQ", [row]),
            _payload("SPY", [spy]),
        ]),
        _Spots(),
        clock=lambda: captured_at,
    ).capture(reviewed_at=REVIEWED, session_date=SESSION)

    assert capture.evidence["selected_at_utc"] == captured_at.isoformat()


def test_advanced_source_rejects_excessive_provider_clock_skew():
    provider_time = REVIEWED.replace(second=6)
    rows = []
    for underlyer, ticker, kind, strike in (
        ("QQQ", "O:QQQ261021C00600000", "call", 600),
        ("SPY", "O:SPY261021P00670000", "put", 670),
    ):
        row = _contract(
            underlyer,
            ticker,
            kind,
            strike,
            strike,
        )
        row["last_quote"]["last_updated"] = int(
            provider_time.timestamp() * 1_000_000_000
        )
        rows.append(_payload(underlyer, [row]))

    with pytest.raises(ValueError, match="clock skew"):
        MassiveAdvancedOptionsSelectionSource(
            _Transport(rows),
            _Spots(),
            clock=lambda: REVIEWED,
        ).capture(reviewed_at=REVIEWED, session_date=SESSION)


def test_exact_contract_source_uses_one_unified_snapshot_request():
    rows = []
    tickers = []
    for underlyer, strike in (("QQQ", 600), ("SPY", 670)):
        for kind, option_type in (("call", "C"), ("put", "P")):
            ticker = (
                f"O:{underlyer}261021{option_type}"
                f"{strike * 1000:08d}"
            )
            row = _contract(
                underlyer,
                ticker,
                kind,
                strike,
                strike,
            )
            row["ticker"] = ticker
            row["details"].pop("ticker")
            row["session"] = row.pop("day")
            rows.append(row)
            tickers.append(ticker)
    transport = _Transport([{
        "status": "OK",
        "request_id": "unified-request",
        "results": list(reversed(rows)),
    }])

    capture = MassiveAdvancedOptionsExactContractSource(
        transport,
    ).capture(
        contract_tickers=tuple(reversed(tickers)),
        reviewed_at=REVIEWED,
        session_date=SESSION,
    )

    assert capture.request_count == capture.pages == 1
    assert capture.pagination_requests == 0
    assert set(capture.selection.contracts) == set(tickers)
    assert capture.evidence["endpoint"] == "/v3/snapshot"
    assert len(transport.calls) == 1
    url, params = transport.calls[0]
    assert url == "https://api.massive.com/v3/snapshot"
    assert params == {
        "ticker.any_of": ",".join(sorted(tickers)),
        "limit": "4",
        "sort": "ticker",
        "order": "asc",
    }


def test_exact_contract_source_rejects_missing_or_paginated_results():
    tickers = (
        "O:QQQ261021C00600000",
        "O:QQQ261021P00600000",
        "O:SPY261021C00670000",
        "O:SPY261021P00670000",
    )
    rows = [
        _contract(
            ticker[2:5],
            ticker,
            "call" if ticker[11] == "C" else "put",
            600 if ticker[2:5] == "QQQ" else 670,
            600 if ticker[2:5] == "QQQ" else 670,
        )
        for ticker in tickers[:-1]
    ]
    with pytest.raises(ValueError, match="scope or pagination"):
        MassiveAdvancedOptionsExactContractSource(
            _Transport([{
                "status": "OK",
                "next_url": "https://api.massive.com/v3/snapshot?cursor=x",
                "results": rows,
            }]),
        ).capture(
            contract_tickers=tickers,
            reviewed_at=REVIEWED,
            session_date=SESSION,
        )


def test_sealed_capture_reconstructs_exact_call_put_scope_provider_free():
    selected_snapshots = {}
    for underlyer, strike in (("QQQ", 600), ("SPY", 670)):
        for kind, option_type in (("call", "C"), ("put", "P")):
            ticker = (
                f"O:{underlyer}261021{option_type}"
                f"{strike * 1000:08d}"
            )
            selected_snapshots[ticker] = _contract(
                underlyer,
                ticker,
                kind,
                strike,
                strike,
            )
    future = REVIEWED + timedelta(seconds=1)
    selected_snapshots[
        "O:SPY261021C00670000"
    ]["last_quote"]["last_updated"] = int(
        future.timestamp() * 1_000_000_000
    )
    capture = p8_activity_selection_capture_from_sealed({
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "compatibility_profile": SEALED_CAPTURE_EXACT_FOUR_PROFILE,
        "configured_underlyings": ["QQQ", "SPY"],
        "captured_at_utc": REVIEWED.isoformat(),
        "session_date": SESSION.isoformat(),
        "provider_requests": 2,
        "decoded_bytes": 4096,
        "responses": [
            {"underlyer": "QQQ", "request_id": "q"},
            {"underlyer": "SPY", "request_id": "s"},
        ],
        "selected_snapshots": selected_snapshots,
        "semantic_sha256": "a" * 64,
    }, compatibility_profile=SEALED_CAPTURE_EXACT_FOUR_PROFILE)

    assert capture.request_count == 2
    assert capture.pagination_requests == 0
    assert capture.selection.reviewed_at == future
    assert capture.selection.contracts == (
        "O:QQQ261021C00600000",
        "O:QQQ261021P00600000",
        "O:SPY261021C00670000",
        "O:SPY261021P00670000",
    )
    assert capture.evidence["sealed_capture_semantic_sha256"] == "a" * 64


def test_sealed_capture_rejects_provider_clock_skew_over_five_seconds():
    selected_snapshots = {}
    for underlyer, strike in (("QQQ", 600), ("SPY", 670)):
        for kind, option_type in (("call", "C"), ("put", "P")):
            ticker = (
                f"O:{underlyer}261021{option_type}"
                f"{strike * 1000:08d}"
            )
            selected_snapshots[ticker] = _contract(
                underlyer,
                ticker,
                kind,
                strike,
                strike,
            )
    selected_snapshots[
        "O:SPY261021C00670000"
    ]["last_quote"]["last_updated"] = int(
        (REVIEWED + timedelta(seconds=6)).timestamp()
        * 1_000_000_000
    )

    with pytest.raises(ValueError, match="clock skew exceeded"):
        p8_activity_selection_capture_from_sealed({
            "contract": "advanced-options-v3-pre-session-capture-v1",
            "compatibility_profile": SEALED_CAPTURE_EXACT_FOUR_PROFILE,
            "configured_underlyings": ["QQQ", "SPY"],
            "captured_at_utc": REVIEWED.isoformat(),
            "session_date": SESSION.isoformat(),
            "provider_requests": 2,
            "decoded_bytes": 4096,
            "responses": [
                {"underlyer": "QQQ", "request_id": "q"},
                {"underlyer": "SPY", "request_id": "s"},
            ],
            "selected_snapshots": selected_snapshots,
            "semantic_sha256": "a" * 64,
        }, compatibility_profile=SEALED_CAPTURE_EXACT_FOUR_PROFILE)


def test_local_replay_cli_is_provider_and_database_free(tmp_path):
    snapshots = {}
    for underlyer, strike in (("QQQ", 600), ("SPY", 670)):
        for kind, option_type in (("call", "C"), ("put", "P")):
            ticker = (
                f"O:{underlyer}261021{option_type}"
                f"{strike * 1000:08d}"
            )
            snapshots[ticker] = _contract(
                underlyer,
                ticker,
                kind,
                strike,
                strike,
            )
    document = {
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "compatibility_profile": SEALED_CAPTURE_EXACT_FOUR_PROFILE,
        "configured_underlyings": ["QQQ", "SPY"],
        "captured_at_utc": REVIEWED.isoformat(),
        "session_date": SESSION.isoformat(),
        "provider_requests": 2,
        "decoded_bytes": 4096,
        "responses": [
            {"underlyer": "QQQ", "request_id": "q"},
            {"underlyer": "SPY", "request_id": "s"},
        ],
        "selected_snapshots": snapshots,
    }
    document["semantic_sha256"] = canonical_sha256(document)
    capture = tmp_path / "capture.json"
    body = (
        json.dumps(document, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    capture.write_bytes(body)
    output = tmp_path / "replay.json"

    result = replay_advanced_options_v3_capture_local.execute(
        SimpleNamespace(
            capture=capture,
            capture_file_sha256=hashlib.sha256(body).hexdigest(),
            output=output,
        )
    )

    assert result["status"] == "PASS"
    assert result["contract_count"] == 4
    assert all(value == 0 for value in result["effects"].values())


def test_explicit_exact_four_rejects_missing_call_put_member():
    snapshots = {}
    for underlyer, strikes in (
        ("QQQ", (600, 601)),
        ("SPY", (670, 671)),
    ):
        for strike in strikes:
            ticker = f"O:{underlyer}261021C{strike * 1000:08d}"
            snapshots[ticker] = _contract(
                underlyer,
                ticker,
                "call",
                strike,
                strikes[0],
            )
    with pytest.raises(ValueError, match="explicit exact-four"):
        p8_activity_selection_capture_from_sealed({
            "contract": "advanced-options-v3-pre-session-capture-v1",
            "compatibility_profile": SEALED_CAPTURE_EXACT_FOUR_PROFILE,
            "configured_underlyings": ["QQQ", "SPY"],
            "captured_at_utc": REVIEWED.isoformat(),
            "session_date": SESSION.isoformat(),
            "provider_requests": 2,
            "decoded_bytes": 4096,
            "responses": [
                {"underlyer": "QQQ"},
                {"underlyer": "SPY"},
            ],
            "selected_snapshots": snapshots,
        }, compatibility_profile=SEALED_CAPTURE_EXACT_FOUR_PROFILE)


def test_generalized_four_contract_capture_allows_one_sided_and_missing_oi():
    snapshots = {}
    for underlyer, kind, option_type, strikes in (
        ("QQQ", "call", "C", (600, 601)),
        ("SPY", "put", "P", (669, 670)),
    ):
        for index, strike in enumerate(strikes):
            ticker = (
                f"O:{underlyer}261021{option_type}"
                f"{strike * 1000:08d}"
            )
            snapshots[ticker] = _contract(
                underlyer,
                ticker,
                kind,
                strike,
                strikes[0],
            )
            if index == 0:
                snapshots[ticker].pop("open_interest")
    capture = p8_activity_selection_capture_from_sealed({
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "compatibility_profile": SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
        "configured_underlyings": ["QQQ", "SPY"],
        "captured_at_utc": REVIEWED.isoformat(),
        "session_date": SESSION.isoformat(),
        "provider_requests": 2,
        "decoded_bytes": 4096,
        "responses": [
            {"underlyer": "QQQ"},
            {"underlyer": "SPY"},
        ],
        "selected_snapshots": snapshots,
    }, compatibility_profile=SEALED_CAPTURE_GENERALIZED_C6_PROFILE)

    assert len(capture.selection.selected) == 4
    assert {
        row.option_type for row in capture.selection.selected
        if row.underlyer == "QQQ"
    } == {"C"}
    assert {
        row.option_type for row in capture.selection.selected
        if row.underlyer == "SPY"
    } == {"P"}
    assert sum(
        row.open_interest is None
        for row in capture.selection.selected
    ) == 2


def test_open_interest_identity_serializes_market_time():
    digest = _open_interest_sha256((
        SimpleNamespace(
            contract_ticker="O:QQQ261021C00700000",
            open_interest=1000,
            market_data_time=REVIEWED,
            source_sha256="a" * 64,
        ),
    ))

    assert len(digest) == 64


def test_p8_manifest_policy_is_same_day_and_bounded():
    policy = _p8_manifest_policy()

    assert policy.target_dtes == (0,)
    assert policy.minimum_dte == policy.maximum_dte == 0
    assert policy.maximum_contracts_per_underlying == 2
    assert policy.maximum_option_quote_contracts == 4


def test_p8_cycle_cutoff_is_prospective_and_window_bounded():
    capture = SimpleNamespace(
        selection=SimpleNamespace(
            valid_until=REVIEWED + timedelta(minutes=20),
        ),
    )

    assert _cycle_cutoff(
        capture,
        REVIEWED,
        REVIEWED + timedelta(minutes=10),
    ) == REVIEWED + timedelta(minutes=9)


def _rejectable_row():
    return _contract(
        "QQQ",
        "O:QQQ261021C00600000",
        "call",
        600,
        600,
    )


@pytest.mark.parametrize(
    ("mutate", "reason"),
    (
        (lambda row: row.pop("last_quote"), "MISSING_LAST_QUOTE"),
        (lambda row: row.pop("day"), "MISSING_DAY"),
        (lambda row: row.pop("details"), "MISSING_DETAILS"),
        (lambda row: row.pop("underlying_asset"), "MISSING_UNDERLYING"),
        (lambda row: row["last_quote"].clear(), "INCOMPLETE_QUOTE"),
        (lambda row: row["last_quote"].pop("ask"), "INCOMPLETE_QUOTE"),
        (
            lambda row: row["last_quote"].pop("last_updated"),
            "INCOMPLETE_QUOTE",
        ),
        (
            lambda row: row["last_quote"].update(bid=0, ask=0),
            "NON_POSITIVE_ASK",
        ),
        (
            lambda row: row["last_quote"].update(bid=2, ask=1),
            "CROSSED_QUOTE",
        ),
        (
            lambda row: row["details"].pop("strike_price"),
            "INCOMPLETE_DETAILS",
        ),
        (
            lambda row: row["underlying_asset"].pop("price"),
            "MISSING_UNDERLYING_PRICE",
        ),
        (
            lambda row: row["details"].update(
                ticker="O:B1QQQ261021C00600000"
            ),
            "NON_STANDARD_ROOT",
        ),
        (
            lambda row: row["underlying_asset"].pop("ticker"),
            "NON_STANDARD_ROOT",
        ),
    ),
)
def test_absent_optional_snapshot_field_rejects_only_its_row(mutate, reason):
    row = _rejectable_row()
    mutate(row)

    assert _snapshot_rejection_reason(row) == reason
    assert _candidate_from_snapshot(row, session_date=SESSION) is None


def test_null_day_volume_is_zero_activity_not_a_failed_capture():
    row = _rejectable_row()
    row["day"]["volume"] = None

    candidate = _candidate_from_snapshot(row, session_date=SESSION)

    assert candidate is not None
    assert candidate.day_volume == 0


def test_rejected_chain_reports_reasons_and_every_real_request():
    zero_quote = _rejectable_row()
    zero_quote["last_quote"].update(bid=0, ask=0)
    no_quote = _rejectable_row()
    no_quote.pop("last_quote")
    first = _payload(
        "QQQ",
        [zero_quote, zero_quote],
        next_url=(
            "https://api.massive.com/v3/snapshot/options/QQQ?cursor=2"
        ),
    )
    second = _payload("QQQ", [no_quote])
    source = MassiveAdvancedOptionsSelectionSource(
        _Transport([first, second]),
        _Spots(),
        clock=lambda: REVIEWED,
        maximum_provider_requests=4,
    )

    with pytest.raises(P8SelectionNoUsableContracts) as raised:
        source.capture(reviewed_at=REVIEWED, session_date=SESSION)

    assert raised.value.rejections == {
        "NON_POSITIVE_ASK": 2,
        "MISSING_LAST_QUOTE": 1,
    }
    assert raised.value.reason_code == (
        "NO_USABLE_CONTRACTS:QQQ:rows=3:"
        "NON_POSITIVE_ASK=2,MISSING_LAST_QUOTE=1"
    )
    assert "no usable contracts for QQQ" in str(raised.value)
    assert source.provider_attempts == (
        P8SelectionProviderAttempt(
            200,
            len(json.dumps(first).encode("utf-8")),
            2,
        ),
        P8SelectionProviderAttempt(
            200,
            len(json.dumps(second).encode("utf-8")),
            1,
        ),
    )


def test_empty_chain_is_distinguishable_from_rejected_rows():
    source = MassiveAdvancedOptionsSelectionSource(
        _Transport([_payload("QQQ", [])]),
        _Spots(),
        clock=lambda: REVIEWED,
    )

    with pytest.raises(P8SelectionNoUsableContracts) as raised:
        source.capture(reviewed_at=REVIEWED, session_date=SESSION)

    assert raised.value.reason_code == "NO_USABLE_CONTRACTS:QQQ:rows=0"
    assert source.provider_attempts[0].row_count == 0


def test_failed_transport_and_http_error_are_still_accounted():
    class _Partial:
        http_status = 200
        response_bytes = 1_234

        def get(self, _url, _params):
            raise TimeoutError("body read timed out")

    class _ServerError:
        def get(self, _url, _params):
            return PolygonHttpResponse(502, b"bad gateway")

    partial = MassiveAdvancedOptionsSelectionSource(
        _Partial(),
        _Spots(),
        clock=lambda: REVIEWED,
    )
    with pytest.raises(TimeoutError):
        partial.capture(reviewed_at=REVIEWED, session_date=SESSION)
    assert partial.provider_attempts == (
        P8SelectionProviderAttempt(200, 1_234),
    )

    rejected = MassiveAdvancedOptionsSelectionSource(
        _ServerError(),
        _Spots(),
        clock=lambda: REVIEWED,
    )
    with pytest.raises(RuntimeError, match="HTTP 502"):
        rejected.capture(reviewed_at=REVIEWED, session_date=SESSION)
    assert rejected.provider_attempts == (
        P8SelectionProviderAttempt(502, len(b"bad gateway")),
    )


def test_other_product_root_in_the_chain_rejects_only_its_rows():
    """Nov 10: the provider listed O:B1SPY..., O:B1VDA... under the same
    underlyings; they must not fail the underlying's whole capture."""
    other = _rejectable_row()
    other["details"]["ticker"] = "O:B1QQQ261021C00600000"
    zero_quote = _rejectable_row()
    zero_quote["last_quote"].update(bid=0, ask=0)
    source = MassiveAdvancedOptionsSelectionSource(
        _Transport([_payload("QQQ", [other, zero_quote])]),
        _Spots(),
        clock=lambda: REVIEWED,
    )

    with pytest.raises(P8SelectionNoUsableContracts) as raised:
        source.capture(reviewed_at=REVIEWED, session_date=SESSION)

    assert raised.value.rejections == {
        "NON_STANDARD_ROOT": 1,
        "NON_POSITIVE_ASK": 1,
    }
