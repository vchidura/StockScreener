"""Offline original Dashboard contracts over an exact realtime-owned daily cohort."""
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from equity.portal_scanners import compute_default_scanner_snapshots
from screeners import _compute_index_technicals

from .contracts import canonical_sha256
from .lineage_head import (
    inputs_bar_lineage_sha256, inputs_sha256, reference_set, require_bounded_manifest, rows_identity,
)
from .portal_projection import build_portal_snapshot
from .preflight import ProviderTier
from .screening_projection import RealtimeScreeningInput, produce_realtime_screening
from .setup_summary_projection import produce_daily_setup_summary


DASHBOARD_VERSION = "DASHBOARD_V1"
DASHBOARD_KEY = "COHORT"
DAILY_SCANNER_CONTRACTS = {
    "gaps": "SCAN_GAPS_1D",
    "fvg": "SCAN_FVG_1D_50",
    "ma-crossover": "SCAN_MA_1D_9_21",
    "momentum-pullback": "SCAN_MOMENTUM_1D",
    "bearish-bounce": "SCAN_BEARISH_1D",
    "fibonacci": "SCAN_FIBONACCI_1D_5",
}
SCANNER_BASIS = dict(
    price="PROVIDER_SPLIT_ADJUSTED_OHLC",
    volume="PROVIDER_ADJUSTED_SERIES_VOLUME",
    raw_volume_parity="UNVERIFIED",
    weekly="DAILY_RESAMPLED_W_FRI_INCLUDES_PARTIAL_CURRENT_WEEK",
    usage="RESEARCH_ONLY_HEURISTIC_GRADES_NOT_PROBABILITIES_OR_RECOMMENDATIONS",
)
MISSING_PRODUCERS = {
    "market_breadth": "FULL_MARKET_UNIVERSE_NOT_RETAINED_COHORT_ONLY",
    "sectors": "DATED_SECTOR_REFERENCE_PRODUCER_UNAVAILABLE",
    "weekly_200": "200_COMPLETED_WEEKS_NOT_RETAINED",
    "market_conditions": "ROTATION_EVENT_MACRO_PRODUCERS_NOT_PART_OF_DASHBOARD",
}
# Per-member lists live in the payload; the manifest keeps only their constant-size identities.
MEMBER_LISTS = ("tickers", "references", "setup_evidence", "missing_setup_confirmations")


def member_set_identities(lists):
    """Manifest identities of the payload member lists; the producer writes them, the reader
    recomputes them from the stored payload. Setup evidence is optional on a retained payload."""
    identities = dict(
        ticker_set=rows_identity(lists["tickers"]),
        reference_set=reference_set(lists["references"]),
        missing_setup_confirmation_set=rows_identity(sorted(lists["missing_setup_confirmations"].items())),
    )
    if lists.get("setup_evidence") is not None:
        identities["setup_evidence_set"] = rows_identity(lists["setup_evidence"])
    return identities


def dashboard_frames(inputs):
    """Use one historical price basis; nominal quotes/liquidity remain separate."""
    frames, raw = {}, {}
    for member in sorted(inputs.members, key=lambda row: row["ticker"]):
        ticker = member["ticker"]
        own = [row for row in inputs.bars if row["ticker"] == ticker and row["interval"] == "1d"]
        adjusted = sorted((row for row in own if row["adjustment_basis"] == "ADJUSTED"),
                          key=lambda row: row["session_date"])
        raw[ticker] = sorted((row for row in own if row["adjustment_basis"] == "RAW_UNADJUSTED"),
                             key=lambda row: row["session_date"])[-20:]
        frames[ticker] = pd.DataFrame(
            [{key: row[key] for key in ("open", "high", "low", "close", "volume")} for row in adjusted],
            index=pd.DatetimeIndex([row["session_date"] for row in adjusted]),
        ).astype(float)
    return frames, raw


def _overview(ticker, frame, raw):
    technicals = _compute_index_technicals(frame, ticker)
    current, previous = raw[-1], float(frame.close.iloc[-2])
    price = float(current["close"])
    high, low = float(frame.high.tail(252).max()), float(frame.low.tail(252).min())
    mean_volume = np.mean([float(row["volume"]) for row in raw])
    return dict(
        ticker=ticker, sector=None, date=str(current["session_date"]),
        open=float(current["open"]), high=float(current["high"]), low=float(current["low"]),
        close=price, volume=int(current["volume"]),
        chg_pct=round((price / previous - 1) * 100, 2),
        rel_vol=round(float(current["volume"]) / mean_volume, 2) if mean_volume > 0 else None,
        high_52w=round(high, 2), low_52w=round(low, 2),
        pct_from_high=round((price / high - 1) * 100, 2),
        pct_from_low=round((price / low - 1) * 100, 2),
        **{key: technicals[key] for key in ("sma_20", "sma_50", "sma_200", "wsma_50", "wsma_200")},
        dist_200=technicals["dist_from_200"],
        dist_200w=None,
    )


def produce_realtime_dashboard(inputs: RealtimeScreeningInput):
    screening = produce_realtime_screening(replace(
        inputs, bars=tuple(row for row in inputs.bars if row["interval"] not in {"1wk", "1mo"}),
    ))
    delay = timedelta(minutes=15 if inputs.provider_tier is ProviderTier.MASSIVE_DEVELOPER else 0)
    indexed = {str(row["bar_revision_id"]): row for row in inputs.bars}

    def source_clock(row):
        clocks = [row["bar_end"] + delay, row["observed_at"], row["created_at"]]
        clocks.extend(row[key] for key in ("replay_available_at", "provider_published_at")
                      if row.get(key) is not None)
        if any(clock.tzinfo is None or clock.utcoffset() is None for clock in clocks):
            raise ValueError("Dashboard source clocks must be timezone-aware")
        return max(clocks)

    # observed_at, created_at, replay_available_at, provider_published_at and the provider-delayed end of a bar
    # are recording clocks (checked against `recorded_by`: known_by, else the cutoff); bar_end is a market time.
    recorded_by = inputs.recorded_by
    for row in inputs.bars:
        available_at = source_clock(row)
        if row["source_kind"] == "DERIVED":
            sources = tuple(indexed[str(source)] for source in row["source_revision_ids"])
            available_at = max(available_at, *(source_clock(source) for source in sources))
        if available_at > recorded_by or row["bar_end"] > inputs.cutoff:
            raise ValueError("Dashboard source availability is beyond cutoff")
    start = min(row["session_date"] for row in inputs.bars if row["interval"] == "1d"
                and row["adjustment_basis"] == "ADJUSTED")
    if any(start < action["effective_date"] <= inputs.session and action["action_type"] != "SPLIT"
           for action in inputs.actions):
        raise ValueError("Dashboard non-split corporate action requires reviewed history")
    frames, raw = dashboard_frames(inputs)
    tickers = sorted(frames)
    scope_note = f"Selected realtime cohort: {len(tickers)} tickers; not full-market or sector coverage."
    scanner_snapshots = compute_default_scanner_snapshots(
        tickers, frames, {ticker: frames[ticker] for ticker in ("SPY", "QQQ") if ticker in frames},
        observed_at=inputs.cutoff,
    )
    scan = scanner_snapshots["SCAN_ALL_1D_5"]
    missing = dict(MISSING_PRODUCERS)
    setup_rows, setup_evidence, missing_confirmations = produce_daily_setup_summary(inputs)
    if missing_confirmations:
        missing["setup_confirmation_1h"] = "EXACT_20_SESSION_1H_CONFIRMATION_UNAVAILABLE"
    if "SPY" not in frames or "QQQ" not in frames:
        missing["market_regime"] = "SPY_QQQ_EXACT_COHORT_INPUTS_UNAVAILABLE"
        scan["market_regime"] = None
    context_rows = []
    for row in screening.rows:
        values = row.payload["values"]
        context_rows.append(dict(
            security_id=str(row.security_id), ticker=row.ticker, sector=None,
            daily=dict(
                status="READY" if values["momentum_percentile"] is not None else "UNAVAILABLE",
                percentile=values["momentum_percentile"], momentum=values["momentum_12_1"],
                price=values["price"], trend=values["discovery_trend"], state=values["discovery_state"],
            ),
            frames={interval: dict(status="UNAVAILABLE", reason="USE_RETAINED_TICKER_WORKSPACE_PUBLICATION")
                    for interval in ("1d", "1h", "30m")},
        ))
    member_lists = dict(
        tickers=tickers,
        references=[dict(member) for member in sorted(inputs.members, key=lambda row: row["ticker"])],
        setup_evidence=setup_evidence, missing_setup_confirmations=missing_confirmations,
    )
    manifest = dict(
        version=DASHBOARD_VERSION, cohort_id=str(inputs.cohort_id), provider_tier=inputs.provider_tier.value,
        source_generation=inputs.source_generation, source_manifest_sha256=inputs.source_manifest_sha256,
        session=str(inputs.session), market_time=screening.manifest["market_time"],
        generated_at=inputs.cutoff.isoformat(), expected_members=len(tickers),
        **member_set_identities(member_lists),
        screening_generation=str(screening.generation_id),
        bar_lineage_sha256=inputs_bar_lineage_sha256(inputs),
        setup_analysis_run_id=setup_evidence[0]["analysis_run_id"],
        input_sha256=inputs_sha256(inputs),
        code_sha256={name: canonical_sha256(path.read_text(encoding="utf-8")) for name, path in (
            ("dashboard", Path(__file__)),
            ("scanners", Path(__file__).resolve().parents[1] / "equity" / "portal_scanners.py"),
            ("screeners", Path(__file__).resolve().parents[1] / "screeners.py"),
            ("setup_summary", Path(__file__).with_name("setup_summary_projection.py")),
            ("ticker_adapter", Path(__file__).with_name("ticker_projection.py")),
            ("materialization", Path(__file__).resolve().parents[1] / "equity" / "materialization.py"),
            ("technicals", Path(__file__).resolve().parents[1] / "equity" / "technicals.py"),
            ("setup_composition", Path(__file__).resolve().parents[1] / "equity" / "setup_composition.py"),
            ("summary_contract", Path(__file__).resolve().parents[1] / "equity" / "ticker_contracts.py"),
        )},
        price_basis="PROVIDER_SPLIT_ADJUSTED_HISTORY_PINNED_CURRENT_RAW_QUOTE",
        liquidity_basis="RAW_LAST_20_SESSIONS_INCLUDING_CURRENT",
        weekly_basis="DAILY_RESAMPLED_W_FRI_50_WEEKS_200_UNAVAILABLE",
        action_coverage="SPLIT_COVERAGE_VERIFIED_OTHER_KNOWN_ACTIONS_ONLY",
        scanner_contracts=list(DAILY_SCANNER_CONTRACTS.values()),
        scanner_basis=SCANNER_BASIS,
    )
    payload = dict(
        version=DASHBOARD_VERSION, cohort_id=str(inputs.cohort_id), session=str(inputs.session),
        market_time=manifest["market_time"], generated_at=inputs.cutoff.isoformat(),
        scope_note=scope_note, missing_producers=missing, **member_lists,
        price_basis=manifest["price_basis"], provider_fetch_performed=False, execution_permission=False,
        overview=[_overview(ticker, frames[ticker], raw[ticker]) for ticker in tickers], scan=scan,
        scanners={key: scanner_snapshots[key] for key in DAILY_SCANNER_CONTRACTS.values()},
        scanner_basis=SCANNER_BASIS,
        setup_summary=dict(
            interval="1d", source="MATERIALIZED_CURRENT_PROJECTION",
            analysis_run_id=manifest["setup_analysis_run_id"], computed_at=inputs.cutoff.isoformat(),
            is_fresh=True, staleness_sessions=0, market_time=manifest["market_time"],
            count=len(setup_rows), results=setup_rows,
        ),
        context=dict(
            schema="stock_equity_context_v1", as_of=inputs.cutoff.isoformat(), status="READY",
            cohort=dict(
                generation=str(screening.generation_id), universe="EXACT_REALTIME_COHORT",
                expected_members=len(tickers), eligible_ranked=screening.manifest["rank_population"],
                session=str(inputs.session), market_time=manifest["market_time"],
                available_at=inputs.cutoff.isoformat(), status="READY",
                ranking="SPLIT_ADJUSTED_12_MINUS_1_COHORT_PERCENTILE", scope_note=scope_note,
            ), rows=context_rows, execution_permission=False,
        ),
    )
    require_bounded_manifest(manifest, name="Dashboard")
    return build_portal_snapshot(
        cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
        snapshot_type=DASHBOARD_VERSION, snapshot_key=DASHBOARD_KEY,
        source_generation=inputs.source_generation, source_manifest=manifest,
        payload=payload, generated_at=inputs.cutoff,
    )
