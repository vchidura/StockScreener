import asyncio
import importlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from realtime.contracts import MarketChannel
from realtime.normalizer import NORMALIZER_VERSION


capture = importlib.import_module(
    "scripts.capture_realtime_v2_advanced_equity_canary"
)
script = importlib.import_module(
    "scripts.provision_realtime_v2_advanced_equity_scope"
)


def test_entitlement_loader_accepts_only_redacted_spy_am_pass(tmp_path):
    path = tmp_path / "entitlement.json"
    path.write_text(
        json.dumps({
            "status": "PASS",
            "checked_at": "2026-10-16T16:35:01+00:00",
            "credentials_printed": False,
            "scope": {
                "authorized_universe": ["SPY"],
                "authorized_channels": ["AM"],
            },
        }),
        encoding="utf-8",
    )

    digest, checked_at = script._load_entitlement(path, ("SPY",))

    assert len(digest) == 64
    assert checked_at == "2026-10-16T16:35:01+00:00"


def test_local_am_canary_emits_provisionable_entitlement_evidence(tmp_path):
    checked_at = datetime(2026, 10, 19, 16, 30, tzinfo=timezone.utc)
    path = tmp_path / "entitlement.json"
    path.write_text(json.dumps({
        "status": "PASS",
        **capture._entitlement_evidence(checked_at),
    }), encoding="utf-8")

    _digest, observed = script._load_entitlement(path, ("SPY",))

    assert observed == checked_at.isoformat()


def test_local_am_canary_emits_exact_target9_entitlement(tmp_path):
    checked_at = datetime(2026, 10, 23, 13, 31, tzinfo=timezone.utc)
    symbols = (
        "AAPL",
        "AMZN",
        "IWM",
        "MSFT",
        "NFLX",
        "NVDA",
        "QQQ",
        "SPY",
        "TSLA",
    )
    path = tmp_path / "target9-entitlement.json"
    path.write_text(json.dumps({
        "status": "PASS",
        **capture._entitlement_evidence(checked_at, symbols),
    }), encoding="utf-8")

    _digest, observed = script._load_entitlement(path, symbols)

    assert observed == checked_at.isoformat()


def test_entitlement_evidence_spans_one_next_session_only():
    observed = datetime(2026, 10, 26, 14, 42, tzinfo=timezone.utc)

    script._validate_entitlement_clock(
        checked=observed + timedelta(hours=23),
        evidence_checked=observed,
    )

    with pytest.raises(ValueError, match="24-hour TTL"):
        script._validate_entitlement_clock(
            checked=observed + timedelta(hours=24, microseconds=1),
            evidence_checked=observed,
        )


def test_local_am_canary_requires_only_bounded_provider_settings():
    assert capture._canary_api_key({
        "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
        "POLYGON_API_KEY": "test-provider-key",
    }) == "test-provider-key"

    with pytest.raises(RuntimeError, match="configured Massive API key"):
        capture._canary_api_key({
            "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
            "POLYGON_API_KEY": "...",
        })


def test_local_am_canary_retains_timeout_for_terminal_report():
    class NeverCompletes:
        async def run_session(self, *, maximum_market_messages):
            assert maximum_market_messages == 90
            await asyncio.sleep(1)

    result, timed_out = asyncio.run(
        capture._capture(NeverCompletes(), 90, 0.001)
    )

    assert result is None
    assert timed_out is True


def test_target9_retry_accepts_only_exact_extended_duration(tmp_path):
    common = [
        "--session-date",
        "2026-10-26",
        "--authorization-reference",
        "MONDAY-TARGET9-LIVE-CANARY-RETRY1-20261026-B0DA3749",
        "--decoded-byte-ceiling",
        "10485760",
        "--maximum-market-messages",
        "90",
        "--journal-path",
        str(tmp_path / "journal.sqlite"),
        "--report-path",
        str(tmp_path / "report.json"),
    ]
    symbols = [
        value
        for symbol in (
            "AAPL",
            "AMZN",
            "IWM",
            "MSFT",
            "NFLX",
            "NVDA",
            "QQQ",
            "SPY",
            "TSLA",
        )
        for value in ("--symbol", symbol)
    ]

    args = capture.parse_args(
        [*common, "--duration-seconds", "660", *symbols]
    )

    assert args.duration_seconds == 660
    with pytest.raises(SystemExit):
        capture.parse_args(
            [*common, "--duration-seconds", "659", *symbols]
        )


def test_target9_terminal_scope_uses_journal_provider_symbol():
    symbols = ("QQQ", "SPY")
    normalized = tuple(
        SimpleNamespace(
            provider_symbol=symbol,
            normalizer_version=NORMALIZER_VERSION,
            event_kind=MarketChannel.MINUTE_AGGREGATE,
            quality="GOOD",
        )
        for symbol in symbols
    )

    assert capture._normalized_scope_matches(
        normalized,
        symbols,
        len(symbols),
    )


def test_entitlement_loader_rejects_broader_or_unredacted_scope(tmp_path):
    path = tmp_path / "entitlement.json"
    path.write_text(
        json.dumps({
            "status": "PASS",
            "checked_at": "2026-10-16T16:35:01+00:00",
            "credentials_printed": True,
            "scope": {
                "authorized_universe": ["SPY", "QQQ"],
                "authorized_channels": ["AM"],
            },
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="exact selected-universe/AM"):
        script._load_entitlement(path, ("SPY",))
