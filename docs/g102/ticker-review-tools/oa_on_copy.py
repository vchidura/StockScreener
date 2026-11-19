"""OA1/OA2 on the real Nov 11 O1 alert rows (served JSON) with valuation inputs read from the copy. Read-only."""
import copy
import json
import sys

import psycopg2
import psycopg2.extras

sys.path.insert(0, "/backend")
from realtime import option_alert_valuation as valuation  # noqa: E402

password = open("/run/secrets/pw", encoding="utf-8").read().strip()
connection = psycopg2.connect(host="g102-test-pg", dbname="test_realtime_v2_r92", user="postgres", password=password,
                              cursor_factory=psycopg2.extras.RealDictCursor)
connection.set_session(readonly=True)
cursor = connection.cursor()
rows = json.load(open("/alerts/alerts-HISTORY.json"))["data"]["rows"]
rows += json.load(open("/alerts/alerts-LATEST.json"))["data"]["rows"]
rows = [copy.deepcopy(row) for row in rows]
inputs = valuation.load_valuation_inputs(cursor, valuation.input_lookups(rows))
print("lookups", len(valuation.input_lookups(rows)), "found", len(inputs))
for row in rows:
    valuation.apply_row(row, valuation.value_row(row, inputs))
    leg = row["original_package"]["legs"][0]
    exit_ = row.get("model_exit") or {}
    print(f"{row['underlyer']:5} {leg['contract_ticker'][-9:]:>9} {row['triggered_at'][11:16]} mark {float(leg['model_mark']):7.3f} "
          f"spot {float(leg['spot']):8.2f} | IV {leg.get('local_iv') or 0:6.3f} d {leg.get('local_delta') or 0:6.3f} "
          f"g {leg.get('local_gamma') or 0:7.4f} {leg.get('greeks_reason') or ''}"
          f" | tgt {exit_.get('target_price')} @{exit_.get('stock_target')} stop {exit_.get('stop_price')} @{exit_.get('stock_stop')}"
          f" exit {exit_.get('target_price_at_exit')} R {exit_.get('reward_risk')} be {exit_.get('breakeven_stock_at_exit')}"
          f" ({exit_.get('breakeven_sigma_at_exit')}σ) {exit_.get('reason') or ''}")
connection.rollback()
