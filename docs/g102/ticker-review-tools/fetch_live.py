"""Read-only GETs against the live portal API. Writes JSON into /out. Usage: fetch_live.py [tickers...]"""
import json
import sys
import time
import urllib.parse
import urllib.request

BASE = "http://stock-screener-realtime-v2-advanced-api:8002/api"


def get(path, **params):
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{BASE}{path}{'?' + query if query else ''}"
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, {"detail": error.read()[:300].decode("utf-8", "replace")}


status, identity = get("/realtime/portal/current")
json.dump(identity, open("/out/portal_current.json", "w"), indent=1)
cohort = identity["equity"]["cohort_id"]
print("cohort", cohort, identity["equity"].get("session"), identity["equity"].get("market_time"))
tickers = sys.argv[1:]
if not tickers:
    _, rows = get("/tickers", cohort_id=cohort)
    rows = rows.get("tickers", rows) if isinstance(rows, dict) else rows
    tickers = sorted({(r["ticker"] if isinstance(r, dict) else r) for r in rows})
print(len(tickers), "tickers")
summary = {}
for ticker in tickers:
    s, pub = get(f"/stock/{ticker}/workspace-publication", read_mode="retained", cohort_id=cohort)
    if s != 200:
        summary[ticker] = {"publication": s, "detail": pub}
        continue
    snap = pub["snapshot_id"]
    s2, multi = get(f"/stock/{ticker}/trade-setup/multi", read_mode="retained", cohort_id=cohort, snapshot_id=snap)
    s3, quote = get(f"/stock/{ticker}/quote", read_mode="retained", cohort_id=cohort, snapshot_id=snap)
    json.dump({"publication": pub, "multi": multi, "quote": quote}, open(f"/out/{ticker}.json", "w"))
    summary[ticker] = {"publication": s, "multi": s2, "quote": s3}
    time.sleep(0.15)
json.dump(summary, open("/out/summary.json", "w"), indent=1)
print(json.dumps({k: v for k, v in summary.items() if v.get("multi") != 200}))
