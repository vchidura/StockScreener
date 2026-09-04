import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.prepare_historical_signal_research import (
    HISTORICAL_BENCHMARK_TICKERS,
    ResponseCache,
    parser,
)


def test_response_cache_reuses_verified_payload(tmp_path) -> None:
    cache = ResponseCache(tmp_path)
    calls = []

    first = cache.get_or_fetch("grouped", "2024-01-02", lambda: calls.append(1) or [{"T": "A"}])
    second = cache.get_or_fetch("grouped", "2024-01-02", lambda: calls.append(2) or [])

    assert first == second == [{"T": "A"}]
    assert calls == [1]
    document = json.loads(next(tmp_path.glob("*.json")).read_text(encoding="utf-8"))
    assert len(document["sha256"]) == 64


def test_cli_requires_explicit_persist_and_defaults_to_100_sessions() -> None:
    dry_run = parser().parse_args(["--dry-run"])
    persist = parser().parse_args(["--persist"])

    assert dry_run.sessions == persist.sessions == 100
    assert dry_run.bar_warmup_sessions is None
    assert dry_run.policy_version == "liquid_us_common_stocks_v2"
    assert dry_run.dry_run is True and dry_run.persist is False
    assert persist.persist is True and persist.dry_run is False
    status = parser().parse_args(["--status"])
    assert status.status is True and status.persist is False


def test_bar_warmup_is_separate_from_universe_lookback() -> None:
    args = parser().parse_args([
        "--persist", "--lookback-sessions", "20",
        "--bar-warmup-sessions", "210", "--backfill-bars",
    ])

    assert args.lookback_sessions == 20
    assert args.bar_warmup_sessions == 210


def test_corporate_actions_and_bar_backfill_require_persistence() -> None:
    actions = parser().parse_args(["--dry-run", "--backfill-actions"])
    bars = parser().parse_args(["--dry-run", "--backfill-bars"])
    sectors = parser().parse_args(["--dry-run", "--backfill-sector-references"])

    assert actions.backfill_actions is True
    assert bars.backfill_bars is True
    assert sectors.backfill_sector_references is True


def test_historical_inputs_include_market_and_all_sector_etfs() -> None:
    assert HISTORICAL_BENCHMARK_TICKERS == frozenset({
        "SPY", "QQQ", "IGV", "SMH", "XLF", "XLV", "XLY", "XLC",
        "XLI", "XLP", "XLE", "XLU", "XLB", "XLRE",
    })