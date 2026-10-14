#!/usr/bin/env python3
"""Offline measurements for the Options Flow publication (v3 review findings 1-3 and 6).

No database, no provider, no network. It measures, on the eight-underlying synthetic fixture:
  bundle       chain validations and wall time: one bundle seal versus one seal per underlying,
  oi           the open-interest change pass against contract count (linear after the v3 fix),
  bytes        the durable forward-input encoding by section (sources, evidence inputs, discovery
               bytes, the per-item underlying-bar tuple) and a projection to the 64 MiB cap,
  cycle_input  (with --cycle-input) the V2 options-cycle input (`realtime.option_cycle_input`, ids and
               hashes only): its encoded bytes at 8, 25 and 50 underlyings for stated contract counts,
               its hard bound, encode/decode time, and the v1 encoder's modelled bytes for the same
               shape. The contract counts are assumptions, listed in the report.
The projections are arithmetic over the measured fixture components and the stated assumptions;
they are not provider measurements. 25/50-underlying end-to-end timing, database growth and reader
latency are NOT measured here (see ADVANCED_REALTIME_SCALING_PLAN.md, E13).
Run from the repository root with the backend virtual environment:
python -B backend/scripts/profile_option_flow_publication.py [--cycle-input]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

BACKEND_DIR = Path(__file__).resolve().parents[1]
for path in (BACKEND_DIR, BACKEND_DIR / "tests" / "realtime"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import realtime.forward_storage as forward_storage  # noqa: E402
import realtime.option_cycle_input as cycle_input  # noqa: E402
import realtime.option_flow_projection as flow  # noqa: E402
from options.domain import ContractType  # noqa: E402
from realtime.preflight import ProviderTier  # noqa: E402
from test_forward_ingestion import MARKET, UnderlyingMinuteBar, cycle  # noqa: E402
from test_option_flow_realtime import flow_chain_evidence, flow_matrices, oi_pairs  # noqa: E402

CAP_BYTES = forward_storage.MAX_FORWARD_INPUT_BYTES


def _fixture():
    build = getattr(cycle, "__wrapped__", None) or cycle._get_wrapped_function()
    return build()


def _size(value) -> int:
    return len(json.dumps(forward_storage._encode(value), sort_keys=True, separators=(",", ":")).encode("ascii"))


def measure_bundle(runs: int) -> dict:
    config, scope, sources = _fixture()
    _, _, matrices, evidence = flow_matrices((config, scope, sources))
    options = dict(selected_at=scope.available_after, chain_evidence=evidence, open_interest_pairs=oi_pairs(scope, matrices))
    calls = []
    original = flow._validate_chain
    flow._validate_chain = lambda *args, **kwargs: (calls.append(1), original(*args, **kwargs))[1]
    try:
        bundle_times, single_times = [], []
        for _ in range(runs):
            calls.clear()
            started = perf_counter()
            flow.seal_offline_option_flow_publications(scope, matrices, config, **options)
            bundle_times.append(perf_counter() - started)
            bundle_validations = len(calls)
            calls.clear()
            started = perf_counter()
            for symbol in scope.underlyings:
                flow.seal_offline_option_flow_publication(scope, matrices, config, underlyer=symbol, **options)
            single_times.append(perf_counter() - started)
            single_validations = len(calls)
    finally:
        flow._validate_chain = original
    return {
        "underlyings": len(scope.underlyings), "bundle_validations": bundle_validations,
        "per_symbol_validations": single_validations,
        "bundle_seconds": round(statistics.median(bundle_times), 4),
        "per_symbol_seconds": round(statistics.median(single_times), 4),
    }


def measure_open_interest(sizes) -> list[dict]:
    _, scope, _ = _fixture()

    class Snapshot:
        def __init__(self, contract_id, symbol, side):
            self.contract_id, self.underlyer, self.contract_type = contract_id, symbol, side
            self.contract_ticker = f"O:{symbol}-{contract_id}"
            self.open_interest, self.batch_id = 100, uuid5(NAMESPACE_URL, "oi-profile")
            self.first_observed_at = scope.available_after

    symbols = tuple(f"S{index:02d}" for index in range(25))
    results = []
    for count in sizes:
        rows = tuple(Snapshot(index + 1, symbols[index % 25], ContractType.CALL if index % 2 else ContractType.PUT)
                     for index in range(count))
        matrices = (SimpleNamespace(snapshots=rows),)
        pairs = oi_pairs(scope, matrices)
        started = perf_counter()
        flow._oi_changes(scope, matrices, pairs, scope.available_after + timedelta(seconds=1))
        results.append({"contracts": count, "seconds": round(perf_counter() - started, 4),
                        "pre_fix_underlyer_reads": count * count})
    return results


def _bars(count: int):
    return tuple(UnderlyingMinuteBar(Decimal("100"), MARKET + timedelta(minutes=index)) for index in range(count))


def measure_bytes(bar_counts) -> dict:
    config, scope, sources = _fixture()
    _, _, matrices, _ = flow_matrices((config, scope, sources))
    source, matrix = sources[0], matrices[0]
    page = source.pages[0]
    items = page.inputs

    def section_sizes(item_count: int, bars: int) -> dict:
        chosen = tuple(replace(item, underlying_bars=_bars(bars)) for item in items[:item_count])
        one = replace(source, pages=(replace(page, inputs=chosen),))
        evidence = flow_chain_evidence(config, scope, matrix, inputs=items[:item_count])
        return {"sources": _size(one), "evidence_inputs": _size(chosen), "discovery": _size(evidence.discovery)}

    one, two = section_sizes(1, 1), section_sizes(2, 1)
    marginal = {key: two[key] - one[key] for key in one}
    fixed = {key: one[key] - marginal[key] for key in one}
    bars_small, bars_large = section_sizes(2, 1), section_sizes(2, 11)
    per_bar_per_item = (bars_large["sources"] - bars_small["sources"]) / (2 * 10)
    text, _ = forward_storage.encode_forward_inputs(
        scope, sources, chain_evidence=flow_matrices((config, scope, sources))[3])
    actual = len(text.encode("ascii"))
    per_underlying_fixed = sum(fixed.values())
    model = lambda underlyings, contracts, bars: int(underlyings * (  # noqa: E731
        per_underlying_fixed + contracts * (marginal["sources"] + marginal["evidence_inputs"]
                                            + marginal["discovery"] + 2 * bars * per_bar_per_item)))
    projections = []
    for underlyings in (8, 25, 50):
        for bars in bar_counts:
            per_item = (marginal["sources"] + marginal["evidence_inputs"] + marginal["discovery"]
                        + 2 * bars * per_bar_per_item)
            allowed = int((CAP_BYTES / underlyings - per_underlying_fixed) // per_item)
            projections.append({"underlyings": underlyings, "bars_per_item": bars,
                                "bytes_per_contract": int(per_item), "contracts_per_underlying_at_cap": allowed})
    return {
        "fixture_actual_bytes": actual, "fixture_model_bytes": model(len(scope.underlyings), 2, 1),
        "fixed_per_underlying": fixed, "marginal_per_contract": marginal,
        "per_bar_per_item_bytes": round(per_bar_per_item, 1), "cap_bytes": CAP_BYTES,
        "projections": projections,
        "model": {"fixed_per_underlying": per_underlying_fixed,
                  "per_item_without_bars": marginal["sources"] + marginal["evidence_inputs"] + marginal["discovery"],
                  "per_bar_per_item_written_twice": 2 * per_bar_per_item},
    }


# Assumptions of the cycle-input report (stated in its output). Contract counts per underlying:
#   12    three-lane forward policy (target DTE 7/21/45, 2 strikes per side: len(target_dtes) * 4),
#   16    four-lane policy (7/21/45/60),
#   1000  the whole provider quote cap (MAXIMUM_SLOT_SET_CONTRACTS) given to one underlying,
#   10000 the discovery evidence row cap per batch (RealtimeOptionDiscoveryEvidence): the full policy chain.
RAW_BYTES_PER_CONTRACT = 2_255  # measured marginal raw payload bytes per fixture item (v1 `sources`)
DISCOVERY_PAGE_ROWS = 250  # rows per provider snapshot page


def _digest(*parts) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()


def synthetic_cycle_input(underlyings: int, contracts: int):
    pages = min(40, max(1, -(-contracts // DISCOVERY_PAGE_ROWS)))
    response_bytes = min(67_108_864, max(1, contracts * RAW_BYTES_PER_CONTRACT))
    entries = tuple(cycle_input.OptionCycleUnderlying(
        underlyer=f"S{index:02d}", underlying_security_id=uuid5(NAMESPACE_URL, f"profile-security:{index}"),
        discovery_batch_id=uuid5(NAMESPACE_URL, f"profile-batch:{index}"),
        discovery_evidence_sha256=_digest("evidence", index), payload_chain_sha256=_digest("payload", index),
        coverage_sha256=_digest("coverage", index), request_filter_sha256=_digest("filter", index),
        page_count=pages, row_count=min(10_000, contracts), response_bytes=response_bytes,
        expiration_from=date(2026, 10, 7), expiration_through=date(2026, 12, 4),
        strike_min=Decimal("85.00"), strike_max=Decimal("115.00")) for index in range(underlyings))
    return cycle_input.OptionCycleInput(
        provider_tier=ProviderTier.MASSIVE_ADVANCED, scheduled_cycle=MARKET,
        profile_id=uuid5(NAMESPACE_URL, "profile-capability"), profile_sha256=_digest("profile"),
        universe_sha256=_digest("universe"), policy_sha256=_digest("policy"),
        configuration_sha256=_digest("configuration"), code_sha256=_digest("code"),
        cohort_id=uuid5(NAMESPACE_URL, "profile-cohort"), underlyings=entries, slot_set_version=1,
        slot_set_member_sha256=_digest("slot-members"))


def _milliseconds(call, runs: int) -> float:
    times = []
    for _ in range(runs):
        started = perf_counter()
        call()
        times.append(perf_counter() - started)
    return round(statistics.median(times) * 1000, 3)


def measure_cycle_input(underlying_counts, contract_counts, bar_counts, runs: int, v1_model: dict) -> dict:
    def v1_bytes(underlyings: int, contracts: int, bars: int) -> int:
        return int(underlyings * (v1_model["fixed_per_underlying"] + contracts * (
            v1_model["per_item_without_bars"] + bars * v1_model["per_bar_per_item_written_twice"])))

    rows = []
    for underlyings in underlying_counts:
        for contracts in contract_counts:
            value = synthetic_cycle_input(underlyings, contracts)
            text, digest = cycle_input.encode_option_cycle_input(value)
            size = len(text.encode("ascii"))
            row = {"underlyings": underlyings, "contracts_per_underlying": contracts, "bytes": size,
                   "bytes_per_underlying": round(size / underlyings, 1),
                   "fraction_of_hard_bound": round(size / cycle_input.MAX_OPTION_CYCLE_INPUT_BYTES, 3),
                   "fraction_of_64_mib_cap": round(size / CAP_BYTES, 6),
                   "encode_ms": _milliseconds(lambda: cycle_input.encode_option_cycle_input(value), runs),
                   "decode_ms": _milliseconds(lambda: cycle_input.decode_option_cycle_input(text, digest), runs),
                   "v1_encoder_model_bytes": {str(bars): v1_bytes(underlyings, contracts, bars) for bars in bar_counts}}
            row["v1_encoder_over_64_mib_cap"] = {bars: size_v1 > CAP_BYTES
                                                  for bars, size_v1 in row["v1_encoder_model_bytes"].items()}
            rows.append(row)
    return {
        "hard_bound_bytes": cycle_input.MAX_OPTION_CYCLE_INPUT_BYTES,
        "maximum_underlyings": cycle_input.MAX_OPTION_CYCLE_UNDERLYINGS,
        "assumptions": {
            "contracts_per_underlying": {"12": "three-lane policy (7/21/45 DTE x 4 contracts)",
                                         "16": "four-lane policy (7/21/45/60 DTE x 4 contracts)",
                                         "1000": "whole provider quote cap on one underlying",
                                         "10000": "discovery row cap per batch: the full policy chain"},
            "raw_bytes_per_contract": RAW_BYTES_PER_CONTRACT, "page_rows": DISCOVERY_PAGE_ROWS,
            "bars_per_item": "not an input of the V2 contract; only the v1 model depends on it",
            "tier_and_widths": "Advanced tier, slot set present, two-digit symbols; page, row and byte counts at their widths",
        },
        "rows": rows,
    }


def measure_cycle_input_fixture() -> dict:
    config, scope, sources = _fixture()
    _, _, _, evidence = flow_matrices((config, scope, sources))
    built = cycle_input.build_option_cycle_input(
        [row.discovery for row in evidence], configuration_sha256=config.configuration_sha256,
        code_sha256=_digest("profile-code"), cohort_id=scope.cohort_id)
    text, _ = cycle_input.encode_option_cycle_input(built)
    legacy, _ = forward_storage.encode_forward_inputs(scope, sources, chain_evidence=evidence)
    return {"underlyings": len(built.underlyings), "contracts_per_underlying": 2,
            "cycle_input_bytes": len(text.encode("ascii")),
            "v1_forward_input_bytes": len(legacy.encode("ascii")),
            "response_bytes_in_cycle_input": 0,
            "discovery_response_bytes_referenced": sum(row.discovery.response_bytes for row in evidence)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--oi-sizes", type=int, nargs="+", default=[1000, 3000, 10000, 30000])
    parser.add_argument("--bars", type=int, nargs="+", default=[1, 390])
    parser.add_argument("--cycle-input", action="store_true",
                        help="also report the V2 options-cycle input bytes at 8/25/50 underlyings")
    parser.add_argument("--underlyings", type=int, nargs="+", default=[8, 25, 50])
    parser.add_argument("--contracts", type=int, nargs="+", default=[12, 16, 1000, 10000])
    args = parser.parse_args()
    report = {"bundle": measure_bundle(args.runs), "oi": measure_open_interest(args.oi_sizes),
              "bytes": measure_bytes(args.bars)}
    if args.cycle_input:
        report["cycle_input"] = measure_cycle_input(args.underlyings, args.contracts, args.bars, max(args.runs, 5),
                                                    report["bytes"]["model"])
        report["cycle_input_fixture"] = measure_cycle_input_fixture()
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
