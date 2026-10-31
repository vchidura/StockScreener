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
    _candidate_from_snapshot,
    p8_activity_selection_capture_from_sealed,
)
from realtime.advanced_options_p8_production import (
    _cycle_cutoff,
    _open_interest_sha256,
    _p8_manifest_policy,
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
        "--authorization-reference",
        "grant-test",
        "--capture-path",
        str(tmp_path / "capture.json"),
        "--report-path",
        str(tmp_path / "report.json"),
    ])

    assert args.maximum_response_decoded_bytes == 1_048_576
    assert args.maximum_decoded_bytes == 18_874_386


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


def test_advanced_source_rejects_pagination_or_incomplete_activity():
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
            next_url="https://api.massive.com/next",
        ),
    ])
    with pytest.raises(ValueError, match="pagination"):
        MassiveAdvancedOptionsSelectionSource(
            paginated,
            _Spots(),
        ).capture(reviewed_at=REVIEWED, session_date=SESSION)

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
    })

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
        })


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


def test_sealed_capture_rejects_missing_call_put_member():
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
    with pytest.raises(ValueError, match="call/put membership"):
        p8_activity_selection_capture_from_sealed({
            "contract": "advanced-options-v3-pre-session-capture-v1",
            "captured_at_utc": REVIEWED.isoformat(),
            "session_date": SESSION.isoformat(),
            "provider_requests": 2,
            "decoded_bytes": 4096,
            "responses": [
                {"underlyer": "QQQ"},
                {"underlyer": "SPY"},
            ],
            "selected_snapshots": snapshots,
        })


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
