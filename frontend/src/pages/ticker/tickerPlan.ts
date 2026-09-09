import { TickerDiscoveryState, TradeSetup } from '../../services/api'
import { MIN_EXECUTABLE_RR, MIN_STOP_ATR, MUTED, NEG, POS, WARN, money, sideOfBias } from './tickerShared'

export interface TradePlan {
  side: 'LONG' | 'SHORT'
  entry: number
  stop: number
  target: number
  stopLabel: string
  targetLabel: string
  risk: number
  reward: number
  riskPct: number
  rewardPct: number
  rr: number
  stopAtr: number | null
}

/** Nearest technical stop and first target around the last close, on the selected interval. */
export function buildTradePlan(setup: TradeSetup): TradePlan | null {
  const side = sideOfBias(setup.direction.bias)
  if (!side) return null

  const entry = setup.last_close
  if (!Number.isFinite(entry) || entry <= 0) return null

  const below = [...(setup.stops ?? [])].filter(s => s.price < entry).sort((a, b) => b.price - a.price)
  const above = [...(setup.targets ?? [])].filter(t => t.price > entry).sort((a, b) => a.price - b.price)
  const atr = setup.technicals.atr

  const shortStops = above.filter(level => level.source !== 'ATR')
  const shortTargets = below.filter(level => level.source !== 'ATR')
  if (Number.isFinite(atr) && atr > 0) {
    shortStops.push({ level: 'ATR Stop (1R)', price: entry + atr, source: 'ATR' })
    if (entry - 2 * atr > 0) {
      shortTargets.push({ level: 'ATR Target (2R)', price: entry - 2 * atr, source: 'ATR' })
    }
  }
  shortStops.sort((a, b) => a.price - b.price)
  shortTargets.sort((a, b) => b.price - a.price)

  const stopPick = side === 'LONG' ? below[0] : shortStops[0]
  const targetPick = side === 'LONG' ? above[0] : shortTargets[0]
  if (!stopPick || !targetPick) return null

  const risk = Math.abs(entry - stopPick.price)
  const reward = Math.abs(targetPick.price - entry)
  if (!Number.isFinite(risk) || risk <= 0) return null

  return {
    side,
    entry,
    stop: stopPick.price,
    target: targetPick.price,
    stopLabel: `${stopPick.level} (${stopPick.source})`,
    targetLabel: `${targetPick.level} (${targetPick.source})`,
    risk,
    reward,
    riskPct: (risk / entry) * 100,
    rewardPct: (reward / entry) * 100,
    rr: reward / risk,
    stopAtr: Number.isFinite(atr) && atr > 0 ? risk / atr : null,
  }
}

interface PlanCheck {
  label: string
  detail: string
  /** Sentence fragment used when this check fails; `detail` describes the state either way. */
  reason: string
  /** null means not applicable, so it is excluded from the score rather than counted as a failure. */
  pass: boolean | null
  blocking?: boolean
}

export interface PlanCaution {
  detail: string
  source: string
}

export interface PlanVerdict {
  label: string
  tone: string
  summary: string
  tooltip: string
  cautions: PlanCaution[]
}

/**
 * Confidence is scored only from computed geometry, volatility and timeframe agreement.
 * Unvalidated signals raise cautions instead, so a shadow heuristic can never hide a plan.
 */
export function evaluatePlan(
  plan: TradePlan,
  setup: TradeSetup,
  livePrice: number | null,
  discovery: TickerDiscoveryState | null,
  discoveryStale: boolean,
): PlanVerdict {
  const drift = livePrice === null
    ? null
    : plan.side === 'LONG' ? livePrice - plan.entry : plan.entry - livePrice
  const confirmTf = setup.ema_alignment.confirm_interval

  const checks: PlanCheck[] = [
    {
      label: 'Reward covers risk',
      pass: plan.rr >= 1,
      blocking: true,
      detail: `${plan.rr.toFixed(2)}R to the first target`,
      reason: `reward is only ${plan.rr.toFixed(2)}R, closer than the stop`,
    },
    {
      label: 'Stop clears noise',
      pass: plan.stopAtr === null ? null : plan.stopAtr >= MIN_STOP_ATR,
      blocking: true,
      detail: plan.stopAtr === null ? 'ATR unavailable' : `${plan.stopAtr.toFixed(2)}× ATR from entry`,
      reason: `the stop sits ${plan.stopAtr?.toFixed(2)}× ATR from entry, inside normal bar range`,
    },
    {
      label: `Meets ${MIN_EXECUTABLE_RR}R floor`,
      pass: plan.rr >= MIN_EXECUTABLE_RR,
      detail: `${plan.rr.toFixed(2)}R`,
      reason: `${plan.rr.toFixed(2)}R is below the ${MIN_EXECUTABLE_RR}R floor`,
    },
    {
      label: 'Higher timeframe agrees',
      pass: setup.ema_alignment.multi_tf_agree,
      detail: setup.ema_alignment.confirm === null
        ? `No ${confirmTf} data to confirm`
        : `${confirmTf} EMA 8/21 ${setup.ema_alignment.confirm.toLowerCase()}`,
      reason: `the ${confirmTf} EMA stack diverges`,
    },
    {
      label: 'Signal confluence',
      pass: /^[AB]/.test(setup.confluence.grade),
      detail: `Grade ${setup.confluence.grade} · ${setup.confluence.count} signals`,
      reason: `confluence is only grade ${setup.confluence.grade} on ${setup.confluence.count} signals`,
    },
    {
      label: 'Entry still valid',
      pass: drift === null ? null : drift <= plan.risk * 0.5,
      detail: drift === null ? 'No live quote' : 'Price is still near the entry',
      reason: `price has run ${money(drift ?? 0)} past the entry`,
    },
  ]

  const cautions: PlanCaution[] = []
  if (discoveryStale) {
    cautions.push({
      detail: 'Daily market state is out of date, so it was left out of this read.',
      source: 'discovery overlay',
    })
  } else if (discovery) {
    const source = `daily discovery overlay · ${discovery.validation_status === 'CANDIDATE_ALPHA' ? 'candidate alpha' : 'unvalidated'}`
    const reversalAgainst = plan.side === 'LONG'
      ? discovery.reversal_trigger?.startsWith('BEARISH')
      : discovery.reversal_trigger?.startsWith('BULLISH')
    const withTrend = (plan.side === 'LONG' && discovery.trend_state === 'UPTREND')
      || (plan.side === 'SHORT' && discovery.trend_state === 'DOWNTREND')
    const againstTrend = (plan.side === 'LONG' && discovery.trend_state === 'DOWNTREND')
      || (plan.side === 'SHORT' && discovery.trend_state === 'UPTREND')
    const extended = !!discovery.extension_risk && discovery.extension_risk !== 'NORMAL'

    if (reversalAgainst) {
      cautions.push({
        detail: discovery.position_guidance
          ?? `Daily reversal trigger ${(discovery.reversal_trigger ?? '').replace(/_/g, ' ').toLowerCase()} runs against this ${plan.side}.`,
        source,
      })
    } else if (withTrend && extended) {
      cautions.push({
        detail: discovery.position_guidance
          ?? `Daily trend is ${discovery.extension_risk?.replace(/_/g, ' ').toLowerCase()} — entering here is chasing.`,
        source,
      })
    }
    // A bias that opposes the daily trend is the falling-knife case, previously unflagged.
    if (againstTrend) {
      cautions.push({
        detail: `Daily trend is ${(discovery.trend_state ?? '').toLowerCase()}, so this ${plan.side} is counter-trend.`,
        source,
      })
    }
  }

  const scored = checks.filter(check => check.pass !== null)
  const failures = scored.filter(check => check.pass === false)
  const blocked = failures.some(check => check.blocking)

  const label = blocked ? 'Not tradeable'
    : failures.length === 0 && cautions.length === 0 ? 'Take'
    : failures.length + cautions.length <= 1 ? 'Take with care'
    : 'Wait'
  const tone = blocked ? NEG
    : label === 'Take' ? POS
    : label === 'Take with care' ? WARN
    : MUTED

  const listed = (blocked ? failures.filter(c => c.blocking) : failures).map(c => c.reason)
  const sentence = (parts: string[]) => parts.length <= 1
    ? parts[0]
    : `${parts.slice(0, -1).join(', ')} and ${parts[parts.length - 1]}`
  const summary = listed.length > 0
    ? `${sentence(listed).replace(/^./, c => c.toUpperCase())}.`
    : `${plan.rr.toFixed(2)}R with the stop ${plan.stopAtr !== null ? `${plan.stopAtr.toFixed(1)}× ATR` : 'clear'} from entry; every check passed.`

  const tooltip = checks
    .map(c => `${c.pass === null ? '–' : c.pass ? '✓' : '✗'} ${c.label} — ${c.detail}`)
    .join('\n')

  return { label, tone, summary, tooltip, cautions }
}
