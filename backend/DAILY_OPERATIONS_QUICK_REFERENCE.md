# 📋 Daily Operations Quick Reference Card

> **STATUS: NOT DEPLOYED — ASPIRATIONAL (as of 2026-08-08)**
>
> The schedule below does not run. `run_scheduler.py` handles price ingestion only and does
> not invoke the recommendation or calibration scripts. No cron or Task Scheduler entry exists.
> See [`docs/SIGNAL_RESEARCH.md`](../docs/SIGNAL_RESEARCH.md) §8.
>
> The one job that *does* run post-close is `job_cross_sectional_signal()`, which produces the
> validated `xsmom-1.0` signal.

---

## 🕒 DAILY SCHEDULE

```
┌─────────────────────────────────────────────────────────────────┐
│                    MARKET OPEN (9:30 AM ET)                     │
│                                                                 │
│  Market opens. Opening patterns become available.               │
│  Market regime scores computed.                                 │
└─────────────────────────────────────────────────────────────────┘

                            ↓ (5 min later)

┌─────────────────────────────────────────────────────────────────┐
│           🟢 9:35 AM: GENERATOR RUNS (Automatic)               │
│                                                                 │
│  ✅ daily_recommendations_generator.py                         │
│  ✅ Generates Top 10 Bull + Top 10 Bear                        │
│  ✅ Stores in daily_recommendations table                      │
│  ✅ Sends notifications to user                                │
│                                                                 │
│  USER ACTION: Review 20 recommendations                        │
│  └─ Decision: Auto-trade or manual?                           │
│  └─ Expected: ~2-3 min for script to complete                │
│                                                                 │
│  DATABASE: 20 new rows inserted                               │
│  OUTPUT: Console summary                                       │
│  ERRORS: Check logs if recommendations missing                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

               Trading Day (9:35 AM - 3:50 PM ET)
           Monitor positions, watch for execution

┌─────────────────────────────────────────────────────────────────┐
│           🔴 4:05 PM: TRACKER RUNS (Automatic)                 │
│                                                                 │
│  ✅ daily_recommendations_tracker.py                           │
│  ✅ Loads today's 20 recommendations                           │
│  ✅ Fetches closing prices                                     │
│  ✅ Compares predicted vs actual returns                       │
│  ✅ Computes win rates & pattern accuracy                      │
│  ✅ Stores performance report                                  │
│                                                                 │
│  USER ACTION: Review performance report                        │
│  └─ Today's win rate: ____%  (Target: ≥55%)                  │
│  └─ Bull accuracy: ____%  (Target: ≥65%)                     │
│  └─ Pattern problems? _____ (Check alerts)                   │
│                                                                 │
│  DATABASE: daily_recommendations updated + new log row        │
│  OUTPUT: Daily performance summary                             │
│  ERRORS: Check logs if prices missing                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  📊 AFTER MARKET (4:10 PM)                      │
│                                                                 │
│  USER REVIEWS: Daily Performance Report                         │
│  ├─ Bull: 7/10 HIT (70%)  ← Expected range: 60-75%            │
│  ├─ Bear: 5/10 HIT (50%)  ← Expected range: 50-65%            │
│  ├─ Overall: 12/20 (60%)  ← Target: ≥55%                      │
│  ├─ Pattern Accuracy: VWAP 72% ✓, Volatility 52% ⚠️          │
│  └─ Confidence Calibration: All bins ±5% ✓                    │
│                                                                 │
│  DECISION: Log findings for weekly review                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              🔄 WEEKLY REVIEW (Friday 5 PM)                    │
│                                                                 │
│  ANALYSIS: Compile 5 days of performance                       │
│  ├─ Win rate trend: _____ (should be stable/improving)        │
│  ├─ Top pattern: _____ (highest accuracy)                     │
│  ├─ Worst pattern: _____ (lowest accuracy)                    │
│  └─ Adjustments needed: _____ (yes/no)                        │
│                                                                 │
│  ACTION: If any pattern < 55% accuracy                        │
│  └─ Insert adjustment record with lower weight               │
│  └─ Update recommendation generator logic                    │
│  └─ Monitor impact next week                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 DAILY CHECKLIST

```
✓ 9:35 AM - Generator Execution
  □ Check console output for "✅ Generated X Bull + Y Bear"
  □ Open database and verify 20 new rows in daily_recommendations
  □ Review top recommendations (any obvious problems?)
  □ Check for auto-trade executions (if enabled)
  □ Document any warnings/errors in daily log

✓ 9:35 - 3:50 PM - Trading Hours
  □ Monitor open positions (if auto-trade enabled)
  □ Watch for profit targets / stop-losses hit
  □ Note any unexpected price movements
  □ Track manually executed recommendations

✓ 4:05 PM - Tracker Execution
  □ Check console output for "✅ Daily tracking complete"
  □ Review performance summary (bull %, bear %, overall %)
  □ Check pattern accuracy breakdown
  □ Note any patterns < 55% (flag for adjustment)
  □ Verify confidence calibration (±5% range)
  □ Check rolling 5-day metric (≥55% target)

✓ 4:10 - 5:00 PM - Report Review
  □ Screenshot daily performance report
  □ Update personal performance tracking spreadsheet
  □ Note any recommendations that completely missed
  □ Document any market anomalies
  □ Check if adjustments are needed

✓ Friday 5 PM - Weekly Review
  □ Compile 5 days of win rates
  □ Calculate rolling 5-day & 20-day metrics
  □ Identify underperforming patterns
  □ Decide on weight adjustments (if needed)
  □ Plan calibration changes for next week
```

---

## 🎯 SUCCESS TARGETS

```
📈 Daily Performance (Each Day)
├─ Overall Win Rate: 55%+ (12+ correct out of 20) ✓
├─ Bull Accuracy: 60%+ (6+ out of 10)
├─ Bear Accuracy: 50%+ (5+ out of 10)
├─ All patterns recorded in database ✓
└─ Confidence calibration tracked ✓

📊 5-Day Rolling (Each Friday)
├─ Win Rate: ≥55% ✓
├─ Bull Accuracy: ≥65%
└─ Consistent day-over-day ✓

📈 Pattern Performance (Track Weekly)
├─ Breakout: 65%+ ← Good
├─ VWAP: 70%+ ← Good
├─ Volatility: 55%+ ← Acceptable, ⚠️ if <55%
├─ Trend: 60%+ ← Good
├─ RS: 60%+ ← Good
└─ Calendar: 55%+ ← Acceptable, ⚠️ if <55%

🎓 Confidence Calibration (Check Weekly)
├─ 90-100% signals: 88% ± 2% actual ✓
├─ 80-90% signals: 82% ± 2% actual ✓
└─ 75-80% signals: 74% ± 2% actual ✓
```

---

## 🚨 ERROR CHECKLIST

```
❌ "No recommendations generated" (0/0)
   → Check: Are opening patterns in database?
   → Fix: Run populate_opening_patterns.py --days 1

❌ "Confidence all 50%" (neutral)
   → Check: Are market regimes computed?
   → Fix: Run market regime analyzer for all indices

❌ "Tracker shows 0% accuracy"
   → Check: Are closing prices in database?
   → Fix: Verify stock_prices_daily table has today's data

❌ "Win rate dropped below 50%"
   → Check: Which patterns are failing?
   → Review: Actual returns vs predictions
   → Decision: Continue or pause auto-trade

❌ "Generator runs but no database insert"
   → Check: Database connection OK?
   → Fix: Run: psql -U postgres -d stock_screener -c "SELECT COUNT(*) FROM daily_recommendations"

❌ "Scheduled job didn't run"
   → Check: Cron job running? (Linux) or Task Scheduler (Windows)?
   → Fix: Run manually: python backend/scripts/daily_recommendations_generator.py
   → Review: Time zone correct? (should be America/New_York)
```

---

## 💾 CRITICAL COMMANDS

```bash
# Generate today's recommendations (manual)
python backend/scripts/daily_recommendations_generator.py

# Track today's performance (manual)
python backend/scripts/daily_recommendations_tracker.py

# Check last 5 days of win rates
psql -U postgres -d stock_screener -c "
  SELECT trade_date, overall_win_rate_pct 
  FROM recommendation_performance_log 
  ORDER BY trade_date DESC LIMIT 5
"

# Check pattern accuracy last week
psql -U postgres -d stock_screener -c "
  SELECT 
    breakout_pattern_accuracy,
    vwap_pattern_accuracy,
    volatility_pattern_accuracy,
    trend_pattern_accuracy,
    rs_pattern_accuracy
  FROM recommendation_performance_log
  ORDER BY report_date DESC LIMIT 7
"

# View today's 20 recommendations
psql -U postgres -d stock_screener -c "
  SELECT ticker, recommendation_type, predicted_return_pct, 
         predicted_confidence_pct, signal_grade 
  FROM daily_recommendations 
  WHERE trade_date = CURRENT_DATE 
  ORDER BY rank_in_category
"

# Check if auto-trade executed
psql -U postgres -d stock_screener -c "
  SELECT ticker, execution_price, executed_at
  FROM daily_recommendations
  WHERE trade_date = CURRENT_DATE AND was_executed = true
"
```

---

## 🎯 WEEKLY DECISION TREE

```
At Friday 5 PM, answer these questions:

1. Did overall win rate meet 55% target?
   ├─ YES → Keep current weights, maintain
   └─ NO → Review underperforming patterns (Q3)

2. Did all patterns have >55% accuracy?
   ├─ YES → No adjustments needed
   └─ NO → Go to Q3

3. Which patterns underperformed (<55%)?
   ├─ Pattern A: 52% accuracy
   │  └─ Decision: Reduce weight by 5 points
   ├─ Pattern B: 48% accuracy
   │  └─ Decision: Reduce weight by 10 points
  └─ Document the decision in the weekly review

4. Is confidence calibration ±5% error?
   ├─ YES → Calibration OK, no adjustment
   └─ NO → Review confidence formula

5. Ready for next week?
   ├─ YES → Auto-trade enabled? Continue
   └─ NO → Wait for calibration improvements
```

---

## 📱 NOTIFICATIONS

```
🔔 Recommended Alerts (Set Up in Email/Slack)

✅ DAILY 9:35 AM
   Subject: "20 Recommendations Generated"
   Check: Any urgent signals? Auto-trade status?

✅ DAILY 4:05 PM
   Subject: "Daily Performance: X% Win Rate"
   Check: Did we hit 55%? Any pattern issues?

⚠️ DAILY (if triggered)
   Subject: "ALERT: Confidence Calibration Issue"
   Action: Review confidence formula

⚠️ DAILY (if triggered)
   Subject: "ALERT: Pattern Accuracy < 55%"
   Action: Flag for weekly adjustment review

📊 WEEKLY Friday 5 PM
   Subject: "Weekly Performance Summary"
   Review: 5-day rolling metrics, plan adjustments
```

---

## 🔄 WEEKLY ADJUSTMENT EXAMPLE

```
FRIDAY REVIEW (2026-08-14):

Current Week Performance:
  Bull: 33/50 (66%) ✓
  Bear: 24/50 (48%) ⚠️
  Overall: 57/100 (57%) ✓

Pattern Accuracy Breakdown:
  Breakout: 68% ✓
  VWAP: 72% ✓
  Volatility: 52% ⚠️ BELOW 55% THRESHOLD
  Trend: 65% ✓
  RS: 60% ✓
  Calendar: 48% ⚠️ BELOW 55% THRESHOLD

DECISION:
  1. Volatility: Reduce weight from 20 → 15 (-5 points)
  2. Calendar: Reduce weight from 15 → 8 (-7 points)
  3. Monitor bear recommendations (48% < 60% target)

IMPLEMENTATION UPDATE:
  Update the recommendation scoring configuration and record the change
  in the weekly review before the next generator run.
  
EXPECTED IMPACT:
  • Volatility-heavy signals now less aggressive
  • Overall signal confidence less volatile
  • Next week: Monitor if bear accuracy improves
```

---

## ✅ DEPLOYMENT CHECKLIST (Before Going Live)

```
WEEK 1: Preparation
□ Migration 004 deployed to production DB
□ 60 days historical data backfilled
□ All backfill scripts completed without errors
□ Test queries run successfully

WEEK 2: Dry Run
□ Generator runs reliably at 9:35 AM
□ Tracker runs reliably at 4:05 PM
□ Daily output matches expected format
□ Database inserts working correctly
□ Win rate ≥55% after 10 trading days

WEEK 3: Integration
□ API endpoint built and tested
□ Frontend component displays recommendations
□ Rolling metrics showing correctly
□ Pattern breakdown visible

WEEK 4: Production
□ 2+ weeks of ≥55% win rate confirmed
□ Confidence calibration stable
□ Auto-trade tested in paper-trading
□ Weekly calibration process working
□ Ready for live trading
```

---

## 🎓 NOTES FOR YOURSELF

```
Remember:
• 9:35 AM generator needs market regime scores → runs after market open
• 4:05 PM tracker needs closing prices → runs after market close
• Win rate target 55% minimum (60%+ is good)
• Any pattern < 55% accuracy needs adjustment
• Confidence calibration should be ±5% error (very tight)
• Bull recommendations typically outperform bear (60-70% vs 50-60%)
• Keep 20 recommendations per day (10/10 split)
• Weekly reviews are critical for continuous improvement
• Don't over-adjust on single day (use 5-day rolling metric)
• Document all changes for future reference
```

---

**Print Date**: ________________  
**Printed By**: ________________  
**Version**: 1.0 (2026-08-08)

