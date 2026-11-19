import json, sys, urllib.request, urllib.parse
B = "http://stock-screener-realtime-v2-advanced-api:8002/api"
cur = json.load(urllib.request.urlopen(B + "/realtime/portal/current", timeout=30))["equity"]["cohort_id"]
out = {}
for t in sys.argv[1:]:
    pub = json.load(urllib.request.urlopen(f"{B}/stock/{t}/workspace-publication?" + urllib.parse.urlencode(dict(read_mode="retained", cohort_id=cur)), timeout=30))
    q = urllib.parse.urlencode(dict(read_mode="retained", cohort_id=cur, snapshot_id=pub["snapshot_id"], report_limit=int(__import__("os").environ.get("LIMIT","12"))))
    prof = json.load(urllib.request.urlopen(f"{B}/equity/security/{t}?{q}", timeout=30))
    reps = prof.get("fundamental_reports") or []
    out[t] = dict(keys=sorted(k for k in prof if k != "fundamental_reports"), status=prof.get("fundamental_reports_status"),
                  available=prof.get("fundamental_reports_available"), n=len(reps),
                  by_tf={tf: sorted({r["period_end"] for r in reps if r["timeframe"] == tf}, reverse=True)[:9] for tf in ("quarterly", "annual", "trailing_twelve_months")})
print(json.dumps(out, indent=1))
