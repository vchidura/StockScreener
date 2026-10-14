"""Loopback-only browser acceptance harness; never opens PostgreSQL or a provider."""
import argparse
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from tests.realtime.offline_io_guard import pytest_configure

pytest_configure(None)

import uvicorn
from tests.realtime.test_screening_vertical_slice import COHORT_ID, screening_app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8014)
    parser.add_argument("--surface", choices=["screening", "ticker", "alerts", "dashboard", "daily-scanners"], default="screening")
    parser.add_argument("--export-path", type=Path)
    args = parser.parse_args()
    if args.surface == "daily-scanners" and args.export_path is None:
        parser.error("Daily scanner validation is export-only; no listener is supported")
    if args.export_path is not None:
        if args.surface not in {"dashboard", "daily-scanners"}:
            parser.error("Static browser export is supported only for the Dashboard fixture")
        from fastapi.testclient import TestClient
        from realtime.contracts import canonical_json
        from tests.realtime.test_dashboard_vertical_slice import dashboard_app
        if args.surface == "daily-scanners":
            from tests.realtime.test_daily_scanner_vertical_slice import daily_scanner_snapshot
            app, _ = dashboard_app(snapshot=daily_scanner_snapshot())
        else:
            app, _ = dashboard_app()
        params = {"cohort_id": str(COHORT_ID)}
        with TestClient(app) as client:
            latest = client.get("/api/latest-price-date", params=params)
            latest.raise_for_status()
            params["snapshot_id"] = latest.json()["publication"]["snapshot_id"]
            payload = {"/api/latest-price-date": latest.json()}
            routes = ["/api/tickers/overview", "/api/tickers/setup-summary", "/api/scan/all", "/api/stocks/context"]
            if args.surface == "daily-scanners":
                from realtime.dashboard_projection import DAILY_SCANNER_CONTRACTS
                routes.extend(f"/api/scan/{contract}" for contract in DAILY_SCANNER_CONTRACTS)
            for route in routes:
                response = client.get(route, params=params)
                response.raise_for_status()
                payload[route] = response.json()
        args.export_path.write_text(canonical_json(payload), encoding="utf-8")
        for route, response in payload.items():
            destination = args.export_path.with_name(f"{args.export_path.stem}.{route.rsplit('/', 1)[-1]}.json")
            destination.write_text(canonical_json(response), encoding="utf-8")
        counts = {route: response["total_signals"] for route, response in payload.items() if "total_signals" in response}
        print(f"SYNTHETIC_{args.surface.upper()}_EXPORTED cohort_id={COHORT_ID}; counts={counts}; no listeners/DB/provider I/O", flush=True)
        return
    print(f"SYNTHETIC_{args.surface.upper()}_STARTING cohort_id={COHORT_ID}", flush=True)
    if args.surface == "dashboard":
        from tests.realtime.test_dashboard_vertical_slice import dashboard_app
        app, _ = dashboard_app()
    elif args.surface == "alerts":
        from tests.realtime.test_stock_alert_vertical_slice import alert_app, retained_snapshot
        app, _ = alert_app(snapshot=retained_snapshot())
    elif args.surface == "ticker":
        from tests.realtime.test_ticker_vertical_slice import ticker_app
        app, _ = ticker_app()
    else:
        app, _, _, _ = screening_app()
    print(f"SYNTHETIC_{args.surface.upper()}_ONLY cohort_id={COHORT_ID}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
