#!/usr/bin/env python3
"""Offline measurement: European implied volatility versus an American-exercise price.

No database, no network. Prices an American option on a Cox-Ross-Rubinstein lattice
(`options.analytics.american_reference`), then solves the production European IV solver
on that price. The difference is the exercise-style bias a consumer inherits when it
compares IV across contracts. Also reports the quote-resolution noise that motivates
the minimum-vega filter in `options.analytics.iv_comparability`.

Run from the repository root:
    backend\\.venv\\Scripts\\python.exe -B backend/scripts/measure_iv_american_bias.py puts
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from decimal import Decimal
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.american_reference import american_price_control_variate  # noqa: E402
from options.analytics.greeks import (  # noqa: E402
    OptionValuationInput,
    price_and_greeks,
    solve_local_greeks,
)
from options.domain import ContractType  # noqa: E402

DEFAULT_RATE = 0.043
SPOT = 600.0
DEFAULT_DAYS = (1, 2, 3, 5, 7, 14, 21, 30, 45, 60, 90)
DEFAULT_VOLATILITIES = (0.15, 0.25, 0.40, 0.60)
PUT_DEPTHS = (-0.10, -0.05, 0.0, 0.0025, 0.005, 0.01, 0.02, 0.03, 0.05, 0.10, 0.15)
CALL_DEPTHS = (0.0, 0.01, 0.02, 0.05, 0.10, 0.15)
CALL_DIVIDEND_YIELDS = (0.0, 0.013, 0.03, 0.043, 0.06, 0.08)
QUOTE_RESOLUTION = 0.005


def european_iv_bias_points(
    contract_type: ContractType,
    *,
    depth: float,
    days: int,
    volatility: float,
    dividend: float,
    steps: int,
    rate: float = DEFAULT_RATE,
) -> float | None:
    """Bias in volatility points. `depth` > 0 is in the money, as a fraction of the forward."""
    maturity = days / 365.0
    forward = SPOT * math.exp((rate - dividend) * maturity)
    strike = round(forward * (1.0 + depth if contract_type is ContractType.PUT else 1.0 - depth), 6)
    price = american_price_control_variate(
        contract_type, spot=SPOT, strike=strike, maturity=maturity, rate=rate,
        dividend=dividend, volatility=volatility, steps=steps,
    )
    result = solve_local_greeks((OptionValuationInput(
        contract_type=contract_type, spot=Decimal(str(SPOT)), strike=Decimal(str(strike)),
        model_mark=Decimal(repr(price)), time_to_expiration_years=maturity,
        risk_free_rate=rate, dividend_yield=dividend,
    ),))[0]
    if not result.converged or result.local_iv is None:
        return None
    return (result.local_iv - volatility) * 100.0


def _grid(contract_type, depths, days, volatilities, dividend, steps, rate):
    table: dict[tuple[float, int], list[float]] = {}
    skipped = 0
    for depth in depths:
        for day in days:
            for volatility in volatilities:
                bias = european_iv_bias_points(
                    contract_type, depth=depth, days=day, volatility=volatility,
                    dividend=dividend, steps=steps, rate=rate,
                )
                if bias is None:
                    skipped += 1
                    continue
                table.setdefault((depth, day), []).append(bias)
    return table, skipped


def _print_table(title, table, depths, days):
    print(f"\n{title}\n  max bias over the volatility grid, volatility points; rows = ITM depth, columns = days")
    print("  depth   " + "".join(f"{day:>7}d" for day in days))
    for depth in depths:
        cells = []
        for day in days:
            values = table.get((depth, day))
            cells.append(f"{max(values, key=abs):>8.2f}" if values else f"{'n/a':>8}")
        print(f"  {depth:>6.2%}" + "".join(cells))


def run_puts(args) -> dict:
    table, skipped = _grid(
        ContractType.PUT, PUT_DEPTHS, args.days, args.volatilities, args.put_dividend, args.steps, args.rate
    )
    _print_table(f"PUTS (q={args.put_dividend:.1%}, r={args.rate:.1%}); negative depth = out of the money", table, PUT_DEPTHS, args.days)
    print(f"  not identifiable / skipped cases: {skipped}")
    return {f"{depth}|{day}": values for (depth, day), values in table.items()}


def run_calls(args) -> dict:
    payload = {}
    for dividend in args.call_dividends:
        table, skipped = _grid(
            ContractType.CALL, args.call_depths, args.days, args.volatilities, dividend, args.steps, args.rate
        )
        _print_table(
            f"CALLS (q={dividend:.1%}, r={args.rate:.1%}, carry r-q={args.rate - dividend:+.1%})",
            table, args.call_depths, args.days,
        )
        print(f"  not identifiable / skipped cases: {skipped}")
        payload[str(dividend)] = {f"{depth}|{day}": values for (depth, day), values in table.items()}
    return payload


def run_noise(args) -> dict:
    """Quote-resolution noise: one half-cent mark step moves IV by resolution / vega."""
    cases = []
    for day in (1, 2, 3, 5, 7, 14, 21, 30, 45, 60):
        maturity = day / 365.0
        forward = SPOT * math.exp((args.rate - 0.013) * maturity)
        for volatility in (0.15, 0.25, 0.40, 0.60):
            for moneyness in (-0.15, -0.10, -0.05, -0.02, 0.0, 0.02, 0.05, 0.10, 0.15):
                strike = round(forward * (1.0 + moneyness), 4)
                contract_type = ContractType.PUT if moneyness <= 0.0 else ContractType.CALL
                price, _, _, _, vega, _ = price_and_greeks(
                    contract_type, spot=SPOT, strike=strike, maturity=maturity,
                    rate=args.rate, dividend=0.013, volatility=volatility,
                )
                cases.append((contract_type, strike, maturity, volatility, price, vega))
    noise = [QUOTE_RESOLUTION / vega if vega > 0 else math.inf for *_, vega in cases]
    ordered = sorted(noise)
    print("\nQUOTE NOISE: IV change caused by one half-cent mark step, volatility points")
    print(f"  grid: +/-15% strike corridor, 1-60 days, 4 volatilities, {len(cases)} cases")
    for share in (0.5, 0.75, 0.9):
        print(f"  p{int(share * 100)} noise {ordered[int(share * len(ordered))]:.3f}")
    for budget in (0.25, 0.5, 1.0, 2.0, 5.0):
        print(
            f"  noise budget {budget:>4.2f} pt -> minimum vega {QUOTE_RESOLUTION / budget:.4f} "
            f"USD/share/vol point keeps {sum(value <= budget for value in noise) / len(noise):.1%}"
        )
    # Check the first-order formula against a real one-step mark move through the solver.
    deviations = []
    for (contract_type, strike, maturity, volatility, price, vega), predicted in zip(cases, noise):
        if predicted > 2.0:
            continue
        moved = solve_local_greeks((OptionValuationInput(
            contract_type=contract_type, spot=Decimal(str(SPOT)), strike=Decimal(str(strike)),
            model_mark=Decimal(repr(price + QUOTE_RESOLUTION)), time_to_expiration_years=maturity,
            risk_free_rate=args.rate, dividend_yield=0.013,
        ),))[0]
        if moved.converged and moved.local_iv is not None:
            deviations.append(abs((moved.local_iv - volatility) * 100.0 - predicted) / predicted)
    print(
        f"  first-order check on {len(deviations)} cases with noise <= 2 pt: "
        f"max relative deviation {max(deviations):.1%}, median {statistics.median(deviations):.2%}"
    )
    return {"cases": len(cases)}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("puts", "calls", "noise", "all"))
    parser.add_argument("--steps", type=int, default=800)
    parser.add_argument("--days", type=int, nargs="+", default=list(DEFAULT_DAYS))
    parser.add_argument("--volatilities", type=float, nargs="+", default=list(DEFAULT_VOLATILITIES))
    parser.add_argument("--put-dividend", type=float, default=0.013)
    parser.add_argument("--call-dividends", type=float, nargs="+", default=list(CALL_DIVIDEND_YIELDS))
    parser.add_argument("--call-depths", type=float, nargs="+", default=list(CALL_DEPTHS))
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    payload = {"rate": args.rate, "spot": SPOT, "steps": args.steps}
    if args.mode in ("puts", "all"):
        payload["puts"] = run_puts(args)
    if args.mode in ("calls", "all"):
        payload["calls"] = run_calls(args)
    if args.mode in ("noise", "all"):
        payload["noise"] = run_noise(args)
    if args.output:
        args.output.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
