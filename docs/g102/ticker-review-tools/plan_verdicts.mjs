// Runs the page's own buildTradePlan + evaluatePlan (worktree src) over served setups, quotes and daily context.
// Also runs a TP3 prototype (stop = nearest level at least 1 ATR away, else the ATR fallback) for comparison.
import { readFileSync, readdirSync } from 'node:fs'
import { build } from 'esbuild'

const compiled = await build({ entryPoints: ['/app/src/pages/ticker/tickerPlan.ts'], bundle: true, write: false,
  platform: 'node', format: 'esm', logLevel: 'silent' })
const { buildTradePlan, evaluatePlan } = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputFiles[0].text).toString('base64')}`)

const tp3b = setup => tp3(setup, true)

// Candidate full rule set: stop = nearest structural level 1-3 ATR away (else the 1 ATR fallback);
// first target = nearest level at least 1R away (else a 2R ATR projection); levels in between are counted.
function tp4(setup) {
  const side = setup.direction.bias === 'Bullish' ? 'LONG' : setup.direction.bias === 'Bearish' ? 'SHORT' : null
  const entry = setup.last_close
  const atr = setup.technicals.atr
  if (!side || !(entry > 0) || !(atr > 0)) return null
  const served = [...(setup.stops ?? []), ...(setup.targets ?? [])].filter(level => level.source !== 'ATR')
  const away = level => Math.abs(level.price - entry)
  const sign = side === 'LONG' ? 1 : -1
  const stopSide = served.filter(level => sign * (level.price - entry) < 0)
  const targetSide = served.filter(level => sign * (level.price - entry) > 0)
  const stop = stopSide.filter(level => away(level) >= 0.995 * atr && away(level) <= 3 * atr)
    .sort((a, b) => away(a) - away(b))[0] ?? { level: 'ATR Stop (1R)', price: entry - sign * atr, source: 'ATR' }
  const risk = away(stop)
  const target = targetSide.filter(level => away(level) >= risk).sort((a, b) => away(a) - away(b))[0]
    ?? { level: 'ATR projection (2R)', price: entry + sign * 2 * risk, source: 'ATR' }
  const reward = away(target)
  const between = targetSide.filter(level => away(level) < reward).length
  return { side, entry, stop: stop.price, target: target.price, stopLabel: `${stop.level} (${stop.source})`,
    targetLabel: `${target.level} (${target.source})`, risk, reward, riskPct: risk / entry * 100,
    rewardPct: reward / entry * 100, rr: reward / risk, stopAtr: risk / atr, between }
}

function tp3(setup, targetBeyondNoise = false) {
  const side = setup.direction.bias === 'Bullish' ? 'LONG' : setup.direction.bias === 'Bearish' ? 'SHORT' : null
  const entry = setup.last_close
  const atr = setup.technicals.atr
  if (!side || !(entry > 0) || !(atr > 0)) return null
  const served = [...(setup.stops ?? []), ...(setup.targets ?? [])].filter(level => level.source !== 'ATR')
  const away = level => Math.abs(level.price - entry)
  const stopSide = served.filter(level => side === 'LONG' ? level.price < entry : level.price > entry)
  const targetSide = served.filter(level => side === 'LONG' ? level.price > entry : level.price < entry)
  const atrStop = { level: 'ATR Stop (1R)', price: side === 'LONG' ? entry - atr : entry + atr, source: 'ATR' }
  const atrTarget = { level: 'ATR Target (2R)', price: side === 'LONG' ? entry + 2 * atr : entry - 2 * atr, source: 'ATR' }
  const stop = stopSide.filter(level => away(level) >= 0.995 * atr).sort((a, b) => away(a) - away(b))[0] ?? atrStop
  const target = [...targetSide, atrTarget].filter(level => level.price > 0)
    .filter(level => !targetBeyondNoise || away(level) >= 0.995 * atr).sort((a, b) => away(a) - away(b))[0]
  const risk = away(stop), reward = away(target)
  return { side, entry, stop: stop.price, target: target.price, stopLabel: `${stop.level} (${stop.source})`,
    targetLabel: `${target.level} (${target.source})`, risk, reward, riskPct: risk / entry * 100,
    rewardPct: reward / entry * 100, rr: reward / risk, stopAtr: risk / atr }
}

const rows = []
for (const file of readdirSync('/live').filter(name => /^[A-Z]+\.json$/.test(name))) {
  const ticker = file.slice(0, -5)
  const { multi, quote } = JSON.parse(readFileSync(`/live/${file}`, 'utf8'))
  const context = JSON.parse(readFileSync(`/ctx/context-${ticker}.json`, 'utf8'))
  const served = context.rows?.length === 1 ? context.rows[0] : null
  // Same row with the daily frame marked READY: isolates geometry from the pre-market UNAVAILABLE status.
  const ready = served && { ...served, frames: { ...served.frames, '1d': { ...served.frames['1d'], status: 'READY' } } }
  for (const [interval, setup] of Object.entries(multi.setups ?? {})) {
    if (!setup) continue
    for (const [rule, planOf] of [['now', buildTradePlan], ['tp3', tp3], ['tp3b', tp3b], ['tp4', tp4]]) {
      const plan = planOf(setup)
      for (const [mode, contextRow] of [['', served], ['+ready', ready]]) {
        const verdict = plan ? evaluatePlan(plan, setup, quote?.price ?? null, contextRow) : null
        rows.push({ rule: rule + mode, ticker, interval, bias: setup.direction.bias, plan, verdict })
      }
    }
  }
}
for (const rule of ['now', 'tp4', 'now+ready', 'tp3+ready', 'tp3b+ready', 'tp4+ready']) {
  const tally = {}
  for (const row of rows.filter(row => row.rule === rule)) {
    const key = `${row.plan?.side ?? 'none'} ${row.verdict?.label ?? 'No plan'}`
    tally[key] = (tally[key] ?? 0) + 1
  }
  console.log(rule, JSON.stringify(tally))
}
const show = (rule, ticker, interval) => {
  const row = rows.find(item => item.rule === rule && item.ticker === ticker && item.interval === interval)
  const p = row.plan
  console.log(`\n[${rule}] ${ticker} ${interval} ${p.side}: entry ${p.entry} stop ${p.stop.toFixed(2)} ${p.stopLabel} | target ${p.target.toFixed(2)} ${p.targetLabel}`)
  console.log(`  Risk $${p.risk.toFixed(2)} (${p.riskPct.toFixed(1)}%)  Reward $${p.reward.toFixed(2)} (${p.rewardPct.toFixed(1)}%)  Ratio ${p.rr.toFixed(2)}R  stop ${p.stopAtr.toFixed(2)}x ATR`)
  console.log(`  ${row.verdict.label}: ${row.verdict.summary}`)
  console.log('  ' + row.verdict.tooltip.split('\n').join('\n  '))
  for (const caution of row.verdict.cautions) console.log(`  caution: ${caution.detail} (${caution.source})`)
}
for (const [ticker, interval] of JSON.parse(process.argv[2] ?? '[]')) { show('now+ready', ticker, interval); show('tp4+ready', ticker, interval) }
for (const rule of ['tp4+ready']) {
  const take = rows.filter(row => row.rule === rule && /^Take/.test(row.verdict?.label ?? ''))
  console.log(`\n${rule} Take/Take with care:`, take.map(row => `${row.ticker} ${row.interval} ${row.plan.side} ${row.plan.rr.toFixed(2)}R ${row.verdict.label}`).join(', '))
  const reasons = {}
  for (const row of rows.filter(item => item.rule === rule && item.verdict?.label === 'Not tradeable')) {
    const key = row.verdict.summary.replace(/[0-9.]+/g, 'n')
    reasons[key] = (reasons[key] ?? 0) + 1
  }
  console.log(`${rule} Not tradeable reasons:`, JSON.stringify(reasons))
}
