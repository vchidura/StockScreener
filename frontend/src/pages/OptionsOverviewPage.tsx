import { useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type FocusEvent, type KeyboardEvent, type PointerEvent, type ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import axios from 'axios'
import { AlertTriangle, BarChart3, CalendarRange, Database, Info, LayoutGrid, ListOrdered, Radio } from 'lucide-react'
import { usePublishPageContext } from '../layout/pageContext'
import { marketSessionOpen, useSessionDate } from '../layout/sessionDate'
import {
  getOptionExposure,
  type OptionExposureExpiry,
  type OptionExposureGreek,
  type OptionExposureRead,
  type OptionExposureScope,
  type OptionExposureStrike,
  type OptionExposureWeight,
} from '../services/api'
import { msUntilWindowRead, OPTIONS_FIRST_CHECK_MS, windowTimeMs } from '../utils/refreshTiming'
import {
  barsExtent,
  cellStrength,
  CONVENTION_NOTE,
  defaultStrikeBand,
  exposureSummary,
  formatCell,
  formatExposure,
  formatPercent,
  formatScaled,
  formatSignedPercent,
  greekName,
  heatmapGrid,
  inUnit,
  labelStride,
  LEVEL_DEFINITIONS,
  markerRows,
  nearestStrikeIndex,
  netLinePath,
  niceTicks,
  opposingSize,
  partsInUnit,
  priceToX,
  qualityNotes,
  regimeText,
  robustMaximum,
  SCOPE_LABELS,
  signLabels,
  strikeBar,
  strikeSegments,
  strikesInBand,
  unavailableText,
  WEIGHT_LABELS,
  zeroDteByStrike,
  zeroDteSplitState,
  type ExposureUnit,
  type ExposureView,
  type HeatmapMode,
  type LevelKind,
  type ZeroDteParts,
} from './optionsExposureModel'
import './OptionsFlowPage.css'
import './OptionsOverviewPage.css'

const GREEKS: OptionExposureGreek[] = ['gamma', 'delta', 'charm', 'vanna']
const WEIGHTS: OptionExposureWeight[] = ['oi', 'oi_volume']
const SCOPES: OptionExposureScope[] = ['0dte', '7d', 'all']
const BANDS: Array<{ value: number | null; label: string }> = [
  { value: 0.025, label: '±2.5%' }, { value: 0.05, label: '±5%' }, { value: 0.1, label: '±10%' }, { value: null, label: 'All' },
]
const INDEX_NAMES: Record<string, string> = { SPY: 'S&P 500 index (SPX)', QQQ: 'Nasdaq-100 index (NDX)', IWM: 'Russell 2000 index (RUT)', DIA: 'Dow Jones index (DJX)' }
const TAG_LABELS: Record<string, string> = { CALL_WALL: 'Call wall', PUT_WALL: 'Put wall', PEAK_GAMMA: 'Peak gamma' }

const pick = <T extends string>(value: string | null, allowed: readonly T[], fallback: T): T =>
  (allowed as readonly string[]).includes(value ?? '') ? value as T : fallback

const formatTime = (value: string | null | undefined) => value
  ? new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }).format(new Date(value)) + ' ET'
  : '—'
const formatDay = (value: string | null | undefined) => value
  ? new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }).format(new Date(`${value}T12:00:00Z`))
  : '—'
const formatStrike = (value: string | number) => Number(value).toLocaleString('en-US', { maximumFractionDigits: 2 })
const formatCount = (value: number | null | undefined) => (value ?? 0).toLocaleString('en-US')

function readError(error: unknown): string {
  if (axios.isAxiosError(error)) {
    if (error.response?.status === 404) return 'The exposure read is not switched on in this API yet. It arrives with the next Options deploy.'
    const detail = (error.response?.data as { detail?: unknown } | undefined)?.detail
    if (typeof detail === 'string') return detail
    return error.message
  }
  return error instanceof Error ? error.message : 'The exposure read failed.'
}

function useElementWidth<T extends HTMLElement>() {
  const ref = useRef<T | null>(null)
  const [width, setWidth] = useState(0)
  useEffect(() => {
    const element = ref.current
    if (!element) return
    setWidth(element.clientWidth)
    const observer = new ResizeObserver(entries => setWidth(Math.floor(entries[0].contentRect.width)))
    observer.observe(element)
    return () => observer.disconnect()
  }, [])
  return [ref, width] as const
}

type Option<T extends string> = { value: T; label: string; disabled?: boolean; title?: string }

function Segmented<T extends string>({ label, value, options, onChange }: {
  label: string; value: T; options: Option<T>[]; onChange: (value: T) => void
}) {
  return (
    <div className="ov-control">
      <span>{label}</span>
      <div className="flow-segmented ov-segmented" role="group" aria-label={label}>
        {options.map(option => (
          <button key={option.value} type="button" className={option.value === value ? 'active' : ''} aria-pressed={option.value === value}
            disabled={option.disabled} title={option.title} onClick={() => onChange(option.value)}>{option.label}</button>
        ))}
      </div>
    </div>
  )
}

/**
 * A definition that opens on hover (mouse), keyboard focus, or a tap/click (which pins it until the next tap, a tap
 * elsewhere, Escape or blur). The bubble is portalled to <body> with fixed coordinates clamped to the viewport, so it
 * never widens the page on a phone. Works for an HTML button and for an SVG group (Enter/Space toggle there).
 */
function useInfoTip<T extends Element>(text: string) {
  const id = useId()
  const triggerRef = useRef<T | null>(null)
  const bubbleRef = useRef<HTMLDivElement | null>(null)
  const [open, setOpen] = useState(false)
  const [pinned, setPinned] = useState(false)
  const [place, setPlace] = useState<{ left: number; top: number } | null>(null)
  const close = useCallback(() => { setOpen(false); setPinned(false); setPlace(null) }, [])
  const position = useCallback(() => {
    const trigger = triggerRef.current, bubble = bubbleRef.current
    if (!trigger || !bubble) return
    // A chart marker's group also holds its line down the plot: anchor at its label instead.
    const rect = (trigger.querySelector('[data-tip-anchor]') ?? trigger).getBoundingClientRect()
    const viewWidth = document.documentElement.clientWidth, viewHeight = window.innerHeight
    const width = bubble.offsetWidth, height = bubble.offsetHeight
    const left = Math.min(Math.max(8, rect.left + rect.width / 2 - width / 2), Math.max(8, viewWidth - width - 8))
    const below = rect.bottom + 6
    const top = below + height > viewHeight - 8 && rect.top - height - 6 >= 8 ? rect.top - height - 6 : below
    setPlace({ left, top })
  }, [])
  useLayoutEffect(() => { if (open) position() }, [open, position])
  useEffect(() => {
    if (!open) return
    const outside = (event: globalThis.PointerEvent) => {
      const target = event.target as Node
      if (!triggerRef.current?.contains(target) && !bubbleRef.current?.contains(target)) close()
    }
    const escape = (event: globalThis.KeyboardEvent) => { if (event.key === 'Escape') close() }
    document.addEventListener('pointerdown', outside)
    document.addEventListener('keydown', escape)
    window.addEventListener('scroll', position, true)
    window.addEventListener('resize', position)
    return () => {
      document.removeEventListener('pointerdown', outside)
      document.removeEventListener('keydown', escape)
      window.removeEventListener('scroll', position, true)
      window.removeEventListener('resize', position)
    }
  }, [open, close, position])
  const toggle = () => { if (pinned) close(); else { setOpen(true); setPinned(true) } }
  const handlers = {
    'aria-describedby': id,
    onPointerEnter: (event: PointerEvent) => { if (event.pointerType === 'mouse') setOpen(true) },
    onPointerLeave: (event: PointerEvent) => { if (event.pointerType === 'mouse' && !pinned) close() },
    // Keyboard focus opens it; a tap or click focuses too, but there the click decides (a tap would otherwise open
    // on focus and close again on the click that follows).
    onFocus: (event: FocusEvent<Element>) => {
      let keyboard = true
      try { keyboard = event.currentTarget.matches(':focus-visible') } catch { /* older browsers: treat as keyboard */ }
      if (keyboard) setOpen(true)
    },
    onBlur: close,
    onClick: toggle,
  }
  // Always mounted (hidden while closed) so aria-describedby has its text; outside the chart's SVG and any transformed
  // ancestor, so fixed coordinates are the viewport's.
  const bubble = text ? createPortal(
    <div ref={bubbleRef} id={id} role="tooltip" className="ov-tip" hidden={!open}
      style={{ left: place?.left ?? 0, top: place?.top ?? 0, visibility: place ? 'visible' : 'hidden' }}>{text}</div>,
    document.body,
  ) : null
  return { triggerRef, handlers, toggle, bubble, open }
}

/** A level name (call wall, put wall, flip) as an inline term with its definition. */
function LevelTerm({ kind, children }: { kind: LevelKind; children: ReactNode }) {
  const tip = useInfoTip<HTMLButtonElement>(LEVEL_DEFINITIONS[kind])
  return (
    <>
      <button type="button" ref={tip.triggerRef} className={`ov-term${tip.open ? ' is-open' : ''}`} {...tip.handlers}>{children}</button>
      {tip.bubble}
    </>
  )
}

/** Today's (0 DTE) and later-expiry parts in the legend (Nov 18). */
function SplitLegend({ split }: { split: 'split' | 'no-today' | 'off' }) {
  if (split === 'split') {
    return <><span><i className="ov-swatch ov-swatch--pos ov-swatch--today" />Expires today (0 DTE)</span><span><i className="ov-swatch ov-swatch--pos" />Later expiries</span></>
  }
  return split === 'no-today' ? <span className="ov-legend__note">Nothing in range expires today</span> : null
}

function Legend({ view, greek, split = 'off', netMarks = false, netLabel = 'Net per strike', note = false }: {
  view: ExposureView; greek: string; split?: 'split' | 'no-today' | 'off'; netMarks?: boolean; netLabel?: string; note?: boolean
}) {
  const signs = signLabels(greek)
  return (
    <div className="ov-legend" aria-label="Legend">
      {view === 'net'
        ? <><span><i className="ov-swatch ov-swatch--pos" />{signs.positive}</span><span><i className="ov-swatch ov-swatch--neg" />{signs.negative}</span></>
        : <><span><i className="ov-swatch ov-swatch--pos" />Calls</span><span><i className="ov-swatch ov-swatch--neg ov-swatch--hatch" />Puts</span></>}
      {view === 'overlay' && <span><i className="ov-swatch-line" />{netLabel}</span>}
      {view === 'net' && netMarks && <span><i className="ov-swatch-dot" />Net where today and later point opposite ways</span>}
      <SplitLegend split={split} />
      {note && <span className="ov-legend__note">{CONVENTION_NOTE}</span>}
    </div>
  )
}

type Marker = { key: string; label: string; x: number; className: string; level?: LevelKind }

/** A level marker's label in the strike chart; flip and walls carry their definition (hover, focus, tap). */
function MarkerLabel({ marker, row, x, left, right, width, plotBottom }: {
  marker: Marker; row: number; x: number; left: number; right: number; width: number; plotBottom: number
}) {
  const level = marker.level
  const tip = useInfoTip<SVGGElement>(level ? LEVEL_DEFINITIONS[level] : '')
  const textX = Math.min(Math.max(x, left + 30), width - right - 30)
  const textY = 10 + row * 15
  const lineAndText = (
    <>
      <line x1={x} x2={x} y1={textY + 4} y2={plotBottom} />
      <text x={textX} y={textY} textAnchor="middle">{marker.label}</text>
    </>
  )
  if (!level) return <g className={`ov-marker ${marker.className}`}>{lineAndText}</g>
  const hitWidth = marker.label.length * 6.2 + 10
  const keys = (event: KeyboardEvent<SVGGElement>) => {
    if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); tip.toggle() }
  }
  return (
    <>
      <g ref={tip.triggerRef} className={`ov-marker ov-marker--tip ${marker.className}${tip.open ? ' is-open' : ''}`}
        tabIndex={0} role="button" {...tip.handlers} onKeyDown={keys}>
        <rect className="ov-marker__hit" data-tip-anchor x={textX - hitWidth / 2} y={textY - 11} width={hitWidth} height={15} rx={2} />
        {lineAndText}
      </g>
      {tip.bubble}
    </>
  )
}

type ChartRow = OptionExposureStrike & { parts: ZeroDteParts | null }

/**
 * Exposure by strike: one bar (Net), stacked call/put segments (Calls vs puts), or those segments with the net per
 * strike as a line (Overlay); with spot, flip and walls marked. When the scope reaches past today, each bar splits into
 * today's (0 DTE) and later expiries (Nov 18).
 */
function StrikeChart({ rows, view, unit, spot, levels, greek, split }: {
  rows: ChartRow[]; view: ExposureView; unit: ExposureUnit; spot: number
  levels: OptionExposureRead['levels']; greek: string; split: 'split' | 'no-today' | 'off'
}) {
  const [containerRef, width] = useElementWidth<HTMLDivElement>()
  const [hover, setHover] = useState<number | null>(null)
  const strikes = rows.map(row => Number(row.strike))
  const left = 66, right = 14, bottom = 30, height = 320
  const plotWidth = Math.max(0, width - left - right)
  const step = rows.length ? plotWidth / rows.length : 0
  const centers = rows.map((_, index) => left + step * (index + 0.5))
  const markers: Marker[] = []
  const addMarker = (key: string, label: string, price: number | null | undefined, className: string, level?: LevelKind) => {
    if (price == null) return
    const x = priceToX(price, strikes, centers)
    if (x != null) markers.push({ key, label, x, className, level })
  }
  addMarker('spot', `Spot ${formatStrike(spot)}`, spot, 'ov-marker--spot')
  if (levels?.flip_spot) addMarker('flip', `Flip ${formatStrike(levels.flip_spot)}`, Number(levels.flip_spot), 'ov-marker--flip', 'flip')
  if (levels?.call_wall) addMarker('call-wall', `Call wall ${formatStrike(levels.call_wall.strike)}`, Number(levels.call_wall.strike), 'ov-marker--wall', 'call_wall')
  if (levels?.put_wall) addMarker('put-wall', `Put wall ${formatStrike(levels.put_wall.strike)}`, Number(levels.put_wall.strike), 'ov-marker--wall', 'put_wall')
  const rowsOfLabels = markerRows(markers.map(marker => marker.x), 104)
  const top = 12 + (Math.max(-1, ...rowsOfLabels) + 1) * 15
  const plotHeight = height - top - bottom
  const parted = split === 'split'
  const bars = rows.map(row => strikeBar(inUnit(row.call, unit, spot), inUnit(row.put, unit, spot), view,
    parted && row.parts ? partsInUnit(row.parts, unit, spot) : null))
  const [rawLow, rawHigh] = barsExtent(bars)
  const low = rawLow * 1.08, high = rawHigh * 1.08
  const ticks = niceTicks(low, high, 5)
  const domainLow = Math.min(low, ticks[0] ?? low), domainHigh = Math.max(high, ticks[ticks.length - 1] ?? high)
  const y = (value: number) => domainHigh === domainLow ? top + plotHeight / 2
    : top + (domainHigh - value) / (domainHigh - domainLow) * plotHeight
  const barWidth = Math.max(1, Math.min(18, step * 0.72))
  const stride = labelStride(rows.length, plotWidth, 46)
  const hovered = hover == null ? null : rows[hover]
  const hoveredParts = parted && hovered?.parts ? hovered.parts : null
  // Net dots: every strike in Overlay (when the strikes are wide enough to tell apart); in Net only where today's and
  // the later part point opposite ways by at least 2 px each, so near-zero strikes do not fill with dots.
  const pixelsPerUnit = domainHigh === domainLow ? 0 : plotHeight / (domainHigh - domainLow)
  const dotted = bars.map(bar => bar.netMarker && (view === 'net' ? opposingSize(bar) * pixelsPerUnit >= 2 : step >= 5))
  const netMarks = dotted.some(Boolean)
  const tooltipWidth = hoveredParts ? 262 : 230

  return (
    <>
      <Legend view={view} greek={greek} split={split} netMarks={view === 'net' && netMarks} note />
      <div className="ov-chart" ref={containerRef}>
        {width > 0 && rows.length > 0 && (
          <svg width={width} height={height} role="img" aria-label={`${greekName(greek)} exposure by strike`}
            onMouseLeave={() => setHover(null)}>
            <defs>
              <pattern id="ov-put-hatch" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
                <rect width="5" height="5" className="ov-hatch-base" /><line x1="0" y1="0" x2="0" y2="5" className="ov-hatch-line" />
              </pattern>
            </defs>
            {ticks.map(tick => (
              <g key={tick} className={tick === 0 ? 'ov-axis ov-axis--zero' : 'ov-axis'}>
                <line x1={left} x2={width - right} y1={y(tick)} y2={y(tick)} />
                <text x={left - 8} y={y(tick)} dy="0.32em" textAnchor="end">{formatScaled(tick, unit)}</text>
              </g>
            ))}
            {rows.map((row, index) => bars[index].segments.map((segment, part) => {
              const start = y(segment.from), end = y(segment.to)
              const gap = segment.from === 0 ? 0 : 1
              const yTop = Math.min(start, end) + (segment.to > segment.from ? 0 : gap)
              const size = Math.max(1, Math.abs(end - start) - gap)
              const tone = segment.kind === 'net' ? (segment.value > 0 ? 'pos' : 'neg') : segment.kind === 'call' ? 'pos' : 'neg ov-bar--hatch'
              return <rect key={`${row.strike}:${part}`} className={`ov-bar ov-bar--${tone}${segment.part === 'today' ? ' is-today' : ''}${hover === index ? ' is-hover' : ''}`}
                x={centers[index] - barWidth / 2} width={barWidth} y={yTop} height={size} rx={Math.min(2, barWidth / 2)} />
            }))}
            {view === 'overlay' && (
              <path className="ov-net-line" d={netLinePath(bars.map((bar, index) => [centers[index], y(bar.net)]))} />
            )}
            {bars.map((bar, index) => dotted[index] && (
              <circle key={`net:${rows[index].strike}`} className={`ov-net-dot${hover === index ? ' is-hover' : ''}`}
                cx={centers[index]} cy={y(bar.net)} r={Math.max(1.5, Math.min(3, barWidth / 3))} />
            ))}
            {rows.map((row, index) => (index % stride === 0 || index === rows.length - 1) && (
              <text key={row.strike} className="ov-axis-label" x={centers[index]} y={height - bottom + 16} textAnchor="middle">{formatStrike(row.strike)}</text>
            ))}
            {rows.map((row, index) => (
              <rect key={`hit:${row.strike}`} className="ov-hit" x={centers[index] - step / 2} width={Math.max(step, 1)} y={top} height={plotHeight}
                onMouseEnter={() => setHover(index)} />
            ))}
            {markers.map((marker, index) => (
              <MarkerLabel key={marker.key} marker={marker} row={rowsOfLabels[index]} x={marker.x} left={left} right={right}
                width={width} plotBottom={top + plotHeight} />
            ))}
          </svg>
        )}
        {hovered && hover != null && (
          <div className={`ov-tooltip${hoveredParts ? ' ov-tooltip--parts' : ''}`} role="status"
            style={{ left: Math.min(centers[hover] + 12, Math.max(0, width - tooltipWidth)), top: top }}>
            <strong>Strike {formatStrike(hovered.strike)}<small>{formatSignedPercent((Number(hovered.strike) - spot) / spot, 2)} from spot</small></strong>
            {hoveredParts ? (
              <div className="ov-tooltip__parts">
                <em /><em>0 DTE</em><em>Later</em><em>Total</em>
                <span><i className="ov-swatch ov-swatch--pos" />Calls</span>
                <b>{formatExposure(hoveredParts.todayCall, unit, spot)}</b><b>{formatExposure(hoveredParts.laterCall, unit, spot)}</b><b>{formatExposure(hovered.call, unit, spot)}</b>
                <span><i className="ov-swatch ov-swatch--neg ov-swatch--hatch" />Puts</span>
                <b>{formatExposure(hoveredParts.todayPut, unit, spot)}</b><b>{formatExposure(hoveredParts.laterPut, unit, spot)}</b><b>{formatExposure(hovered.put, unit, spot)}</b>
                <span className="ov-tooltip__net">Net</span>
                <b className="ov-tooltip__net">{formatExposure(hoveredParts.todayCall + hoveredParts.todayPut, unit, spot)}</b>
                <b className="ov-tooltip__net">{formatExposure(hoveredParts.laterCall + hoveredParts.laterPut, unit, spot)}</b>
                <b className="ov-tooltip__net">{formatExposure(hovered.call + hovered.put, unit, spot)}</b>
              </div>
            ) : (
              <>
                <span><i className="ov-swatch ov-swatch--pos" />Calls<b>{formatExposure(hovered.call, unit, spot)}</b></span>
                <span><i className="ov-swatch ov-swatch--neg ov-swatch--hatch" />Puts<b>{formatExposure(hovered.put, unit, spot)}</b></span>
                <span className="ov-tooltip__net">Net<b>{formatExposure(hovered.call + hovered.put, unit, spot)}</b></span>
              </>
            )}
            {hoveredParts && <span className="ov-tooltip__oi">0 DTE expires today at 4:00 PM ET; later = every later expiry in range</span>}
            <span className="ov-tooltip__oi">OI calls {formatCount(hovered.call_open_interest)} · puts {formatCount(hovered.put_open_interest)}</span>
          </div>
        )}
        {!rows.length && <p className="ov-empty">No strikes with exposure in this window.</p>}
      </div>
    </>
  )
}

function StrikeTable({ rows, unit, spot }: { rows: OptionExposureStrike[]; unit: ExposureUnit; spot: number }) {
  return (
    <div className="flow-table-wrap ov-strike-table">
      <table className="flow-table">
        <thead><tr><th>Strike</th><th>Calls</th><th>Puts</th><th>Net</th><th>Call OI</th><th>Put OI</th><th>From spot</th></tr></thead>
        <tbody>{[...rows].reverse().map(row => (
          <tr key={row.strike}><td>{formatStrike(row.strike)}</td><td>{formatExposure(row.call, unit, spot)}</td><td>{formatExposure(row.put, unit, spot)}</td>
            <td><strong>{formatExposure(row.net, unit, spot)}</strong></td><td>{formatCount(row.call_open_interest)}</td><td>{formatCount(row.put_open_interest)}</td>
            <td>{formatSignedPercent((Number(row.strike) - spot) / spot, 2)}</td></tr>
        ))}</tbody>
      </table>
    </div>
  )
}

/** Strike x expiry heatmap: strikes high to low, expiries left to right, diverging colour around zero, spot marked. */
function Heatmap({ data, band, mode, unit, spot }: {
  data: OptionExposureRead; band: number | null; mode: HeatmapMode; unit: ExposureUnit; spot: number
}) {
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const strikes = (data.strikes ?? []).map(row => row.strike)
  const expiries = data.expiries ?? []
  const matrix = useMemo(() => heatmapGrid(data.grid ?? [], (data.strikes ?? []).map(row => row.strike),
    (data.expiries ?? []).map(row => row.expiration_date), mode), [data.grid, data.strikes, data.expiries, mode])
  const parts = useMemo(() => {
    const found = new Map<string, [number, number]>()
    for (const [strike, expiry, call, put] of data.grid ?? []) found.set(`${strike}:${expiry}`, [call, put])
    return found
  }, [data.grid])
  const shown = strikes.map((strike, index) => ({ strike, index })).filter(({ strike }) =>
    band == null || Math.abs(Number(strike) - spot) <= spot * band).reverse()
  const maximum = robustMaximum(shown.flatMap(({ index }) => matrix.values[index]))
  const spotIndex = nearestStrikeIndex(shown.map(row => row.strike), spot)

  useEffect(() => {
    const container = scrollRef.current
    const row = container?.querySelector<HTMLElement>('tr.is-spot')
    if (container && row) container.scrollTop = Math.max(0, row.offsetTop - container.clientHeight / 2)
  }, [data.observed_at, data.underlyer, band, mode])

  if (!shown.length) return <p className="ov-empty">No strikes with exposure in this window.</p>
  return (
    <div className="ov-heatmap" ref={scrollRef}>
      <table aria-label={`${greekName(data.greek)} exposure by strike and expiry`}>
        <thead><tr><th scope="col" className="ov-heatmap__corner">Strike</th>{expiries.map(row => (
          <th scope="col" key={row.expiration_date}>{formatDay(row.expiration_date)}<small>{row.calendar_dte}d</small></th>
        ))}</tr></thead>
        <tbody>{shown.map(({ strike, index }, position) => (
          <tr key={strike} className={position === spotIndex ? 'is-spot' : undefined}>
            <th scope="row">{formatStrike(strike)}{position === spotIndex && <small>spot {formatStrike(spot)}</small>}</th>
            {matrix.values[index].map((value, expiryIndex) => {
              if (value == null) return <td key={expiryIndex} className="ov-cell ov-cell--empty" />
              const strength = cellStrength(value, maximum)
              const [call, put] = parts.get(`${index}:${expiryIndex}`) ?? [0, 0]
              const pole = value >= 0 ? 'var(--ov-pos)' : 'var(--ov-neg)'
              return (
                <td key={expiryIndex} className={`ov-cell${strength > 0.55 ? ' is-strong' : ''}`}
                  style={{ background: `color-mix(in oklab, ${pole} ${Math.round(strength * 82)}%, var(--ov-mid))` }}
                  title={`Strike ${formatStrike(strike)} · ${formatDay(expiries[expiryIndex].expiration_date)} (${expiries[expiryIndex].calendar_dte} DTE)\nNet ${formatExposure(call + put, unit, spot)} · calls ${formatExposure(call, unit, spot)} · puts ${formatExposure(put, unit, spot)}`}>
                  {formatCell(value, unit, spot)}
                </td>
              )
            })}
          </tr>
        ))}</tbody>
      </table>
    </div>
  )
}

function ExpiryStrip({ rows, view, unit, spot }: { rows: OptionExposureExpiry[]; view: ExposureView; unit: ExposureUnit; spot: number }) {
  const reach = Math.max(1, ...rows.flatMap(row => view === 'net' ? [Math.abs(row.net)] : [
    Math.max(0, row.call) + Math.max(0, row.put), Math.max(0, -row.call) + Math.max(0, -row.put)]))
  const width = (value: number) => `${Math.abs(value) / reach * 50}%`
  return (
    <div className="ov-expiry">
      {rows.map(row => (
        <div className="ov-expiry__row" key={row.expiration_date}
          title={`${formatDay(row.expiration_date)}: net ${formatExposure(row.net, unit, spot)} · calls ${formatExposure(row.call, unit, spot)} · puts ${formatExposure(row.put, unit, spot)} · ${formatPercent(row.share)} of the range's gross exposure · ${row.contracts} contracts`}>
          <div className="ov-expiry__date"><strong>{formatDay(row.expiration_date)}</strong><small>{row.calendar_dte} DTE</small></div>
          <div className="ov-expiry__track">
            <span className="ov-expiry__zero" />
            {view === 'net'
              ? <span className={`ov-expiry__bar ov-expiry__bar--${row.net >= 0 ? 'pos' : 'neg'}`}
                  style={row.net >= 0 ? { left: '50%', width: width(row.net) } : { right: '50%', width: width(row.net) }} />
              : strikeSegments(row.call, row.put, 'split').map(segment => (
                <span key={segment.kind} className={`ov-expiry__bar ov-expiry__bar--${segment.kind === 'call' ? 'pos' : 'neg ov-bar--hatch'}`}
                  style={segment.value >= 0
                    ? { left: `calc(50% + ${Math.abs(segment.from) / reach * 50}%)`, width: width(segment.value) }
                    : { right: `calc(50% + ${Math.abs(segment.from) / reach * 50}%)`, width: width(segment.value) }} />
              ))}
            {view === 'overlay' && <span className="ov-expiry__net" style={{ left: `${50 + row.net / reach * 50}%` }} />}
          </div>
          <div className="ov-expiry__value"><strong>{formatExposure(row.net, unit, spot)}</strong><small>{formatPercent(row.share)} of gross</small></div>
        </div>
      ))}
    </div>
  )
}

/**
 * The factual summary line at the top of the page (Nov 18): flip and regime at spot, walls, today's expiring share and
 * the capture time (stale after 10 minutes while the market is open). Re-checks the clock every 30 s so the stale
 * mark appears even when no new capture arrives.
 */
function SummaryLine({ data }: { data: OptionExposureRead }) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 30_000)
    return () => window.clearInterval(timer)
  }, [])
  const summary = exposureSummary(data, { nowMs: now, marketOpen: marketSessionOpen({ session_date: data.session_date }, new Date(now)) })
  return (
    <section className={`ov-brief${summary.stale ? ' is-stale' : ''}`} aria-label={`${data.underlyer} summary`}>
      <p>{summary.tokens.map((token, index) => token.term
        ? <LevelTerm key={index} kind={token.term}>{token.text}</LevelTerm>
        : token.stale
          ? <strong key={index} className="ov-brief__stale">{token.text}</strong>
          : <span key={index}>{token.text}</span>)}</p>
    </section>
  )
}

function Section({ icon, title, detail, aside, children }: { icon: ReactNode; title: string; detail: string; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="flow-section ov-section">
      <header className="flow-section__header"><div>{icon}<div><h2>{title}</h2><p>{detail}</p></div></div>{aside}</header>
      {children}
    </section>
  )
}

export default function OptionsOverviewPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const underlyer = (searchParams.get('underlyer') || 'SPY').toUpperCase()
  const greek = pick(searchParams.get('greek'), GREEKS, 'gamma')
  const weight = pick(searchParams.get('weight'), WEIGHTS, 'oi')
  const scope = pick(searchParams.get('scope'), SCOPES, 'all')
  const view = pick<ExposureView>(searchParams.get('view'), ['net', 'split', 'overlay'], 'net')
  const unit = pick<ExposureUnit>(searchParams.get('unit'), ['usd', 'shares'], 'usd')
  const { pinned: sessionDate } = useSessionDate()
  const [bandChoice, setBandChoice] = useState<number | null | undefined>(undefined)
  const [heatmapMode, setHeatmapMode] = useState<HeatmapMode>('net')
  const [showTable, setShowTable] = useState(false)
  const known = useRef<string[]>([])
  const band = bandChoice === undefined ? defaultStrikeBand(underlyer) : bandChoice

  const setParam = (name: string, value: string) => setSearchParams(current => {
    const next = new URLSearchParams(current)
    next.set(name, value)
    return next
  }, { replace: true })

  // Read when the next 5-minute chain capture lands (first check 50 s after each mark), 60 s when none is expected.
  const exposure = useQuery({
    queryKey: ['options', 'exposure', underlyer, greek, weight, scope, sessionDate],
    queryFn: () => getOptionExposure({ underlyer, greek, weight, scope, sessionDate: sessionDate || undefined }),
    placeholderData: keepPreviousData,
    refetchInterval: current => msUntilWindowRead(Date.now(), windowTimeMs(current.state.data?.observed_at), OPTIONS_FIRST_CHECK_MS, 60_000),
    retry: (count, error) => !(axios.isAxiosError(error) && [404, 422].includes(error.response?.status ?? 0)) && count < 2,
  })
  const data = exposure.data
  if (data?.underlyers?.length) known.current = data.underlyers
  const underlyers = known.current.length ? known.current : [underlyer]
  const enabled = new Set(data?.greeks_enabled ?? ['gamma', 'delta'])
  const spot = Number(data?.spot ?? 0)

  usePublishPageContext({
    eyebrow: 'Dealer positioning · research observation',
    title: 'Options Overview',
    detail: 'Gamma and delta exposure by strike and expiry from the stored option chain, under a stated dealer convention.',
    session: '5m',
  })

  const controls = (
    <section className="ov-command" aria-label="Options Overview controls">
      <label className="ov-control ov-control--select"><span>Underlying</span>
        <select value={underlyer} onChange={event => setParam('underlyer', event.target.value)}>
          {(underlyers.includes(underlyer) ? underlyers : [underlyer, ...underlyers]).map(name => <option key={name} value={name}>{name}</option>)}
        </select>
      </label>
      <Segmented label="Greek" value={greek} onChange={value => setParam('greek', value)} options={GREEKS.map(item => ({
        value: item, label: greekName(item), disabled: !enabled.has(item),
        title: enabled.has(item) ? undefined : 'Second-order Greeks (phase 2): not available yet',
      }))} />
      <Segmented label="View" value={view} onChange={value => setParam('view', value)} options={[{ value: 'net', label: 'Net' }, { value: 'split', label: 'Calls vs puts' },
        { value: 'overlay', label: 'Overlay', title: 'Calls vs puts bars with the net per strike as a line on top' }]} />
      <Segmented label="Weight" value={weight} onChange={value => setParam('weight', value)} options={WEIGHTS.map(item => ({ value: item, label: WEIGHT_LABELS[item] }))} />
      <Segmented label="Expiries" value={scope} onChange={value => setParam('scope', value)} options={SCOPES.map(item => ({ value: item, label: SCOPE_LABELS[item] }))} />
      <Segmented label="Units" value={unit} onChange={value => setParam('unit', value)} options={[{ value: 'usd', label: 'Dollars' }, { value: 'shares', label: 'Shares' }]} />
    </section>
  )

  if (exposure.isLoading) return <div className="options-flow-page">{controls}<div className="flow-state"><Database size={20} /><div><strong>Loading exposure</strong><span>Reading the latest stored chain capture for {underlyer}.</span></div></div></div>
  if (exposure.isError && !data) return <div className="options-flow-page">{controls}<div className="flow-state flow-state--warning"><AlertTriangle size={20} /><div><strong>Options Overview unavailable</strong><span>{readError(exposure.error)}</span></div></div></div>
  if (!data) return null
  if (!data.available) {
    const text = unavailableText(data.reason, data.underlyer, data.scope)
    return (
      <div className="options-flow-page">
        <SummaryLine data={data} />
        {controls}
        <div className="flow-state flow-state--warning" role="status"><AlertTriangle size={20} /><div><strong>{text.title}</strong>
          <span>{text.detail}{data.observed_at ? ` Chain captured ${formatTime(data.observed_at)}.` : ''}</span>
          {text.tryScope && <button type="button" className="ov-link" onClick={() => setParam('scope', text.tryScope!)}>Show {SCOPE_LABELS[text.tryScope]}</button>}
        </div></div>
      </div>
    )
  }

  const levels = data.levels
  const totals = data.totals!
  const coverage = data.coverage!
  const cross = data.cross_check
  // Nov 18: today's (0 DTE) and later parts of each strike, from the strike x expiry cells, when the scope reaches past today.
  const split = zeroDteSplitState(data.scope, data.expiries ?? [])
  const strikeParts = split === 'split' ? zeroDteByStrike(data.grid ?? [], data.expiries ?? [], data.strikes?.length ?? 0) : null
  const strikeRows: ChartRow[] = strikesInBand((data.strikes ?? []).map((row, index) => ({ ...row, parts: strikeParts?.[index] ?? null })), spot, band)
  const notes = qualityNotes(data.quality_reasons, {
    underlyer: data.underlyer, providerIvContracts: coverage.provider_iv_contracts, weightCoverage: coverage.weight_coverage_fraction,
    crossCheck: cross?.difference_fraction, crossCheckNet: cross?.net_difference_fraction,
    spotGap: coverage.spot_check?.gap_fraction, quotePrice: coverage.spot_check?.quote_underlying_price, spot: data.spot,
  })
  const warnings = notes.filter(note => note.tone === 'warning')
  const indexName = INDEX_NAMES[data.underlyer]
  const name = greekName(data.greek)
  const bandLabel = BANDS.find(item => item.value === band)?.label ?? 'All'

  return (
    <div className="options-flow-page ov-page">
      <SummaryLine data={data} />
      {controls}
      <section className="flow-command__context ov-stamp" aria-label="Data stamp">
        <Radio size={15} />
        <span><strong>{data.underlyer} · spot {formatStrike(spot)} · chain captured {formatTime(data.observed_at)}{exposure.isFetching && exposure.isPlaceholderData ? ' · updating…' : ''}</strong>
          {formatCount(totals.contracts)} of {formatCount(coverage.capture_contracts)} captured contracts carry weight and a Greek in {SCOPE_LABELS[data.scope].toLowerCase()} · strikes {formatStrike(coverage.strike_low ?? 0)}–{formatStrike(coverage.strike_high ?? 0)} ({coverage.strike_band_fraction ? `${formatSignedPercent(coverage.strike_band_fraction[0], 0)} / ${formatSignedPercent(coverage.strike_band_fraction[1], 0)}` : '—'} of spot) · {coverage.expiry_count} expiries {formatDay(coverage.expiry_first)} – {formatDay(coverage.expiry_last)}
        </span>
      </section>
      {exposure.isError && <div className="flow-state flow-state--warning" role="status"><AlertTriangle size={20} /><div><strong>Latest read failed</strong><span>{readError(exposure.error)} Showing the last view that loaded: {data.underlyer} {greekName(data.greek).toLowerCase()}, {SCOPE_LABELS[data.scope]}.</span></div></div>}
      {warnings.map(note => <div key={note.text} className="flow-state flow-state--warning" role="status"><AlertTriangle size={20} /><div><strong>Check before reading</strong><span>{note.text}</span></div></div>)}

      <section className="flow-summary ov-summary" aria-label={`${data.underlyer} exposure summary`}>
        <div><span>Net {name.toLowerCase()} · {SCOPE_LABELS[data.scope]}</span><strong>{formatExposure(totals.net, unit, spot)}</strong>
          <small><i className="ov-text-pos">calls {formatExposure(totals.call, unit, spot)}</i> · <i className="ov-text-neg">puts {formatExposure(totals.put, unit, spot)}</i></small></div>
        <div><span>Gamma at spot</span><strong>{regimeText(levels?.regime_at_spot)}</strong>
          <small>{levels?.flip_spot
            ? <><LevelTerm kind="flip">Flip</LevelTerm> {formatStrike(levels.flip_spot)} ({formatSignedPercent(levels.flip_distance_to_spot_fraction, 2)})</>
            : <>No <LevelTerm kind="flip">flip</LevelTerm> within ±20% of spot</>}</small></div>
        <div><span><LevelTerm kind="call_wall">Call wall</LevelTerm></span><strong>{levels?.call_wall ? formatStrike(levels.call_wall.strike) : '—'}</strong>
          <small>{levels?.call_wall ? `${formatSignedPercent(levels.call_wall.distance_to_spot_fraction, 2)} · ${formatExposure(levels.call_wall.gamma_notional_per_percent, 'usd', spot, false)} gamma per 1%` : 'No call gamma'}</small></div>
        <div><span><LevelTerm kind="put_wall">Put wall</LevelTerm></span><strong>{levels?.put_wall ? formatStrike(levels.put_wall.strike) : '—'}</strong>
          <small>{levels?.put_wall ? `${formatSignedPercent(levels.put_wall.distance_to_spot_fraction, 2)} · ${formatExposure(levels.put_wall.gamma_notional_per_percent, 'usd', spot, false)} gamma per 1%` : 'No put gamma'}</small></div>
        <div><span>Expiring today</span><strong>{formatPercent(data.expiring_today_share)}</strong><small>of all captured {name.toLowerCase()} exposure (gross)</small></div>
        <div><span>Provider Greeks check</span><strong>{formatSignedPercent(cross?.difference_fraction)}</strong>
          <small>gross, {formatCount(cross?.contracts_compared)} contracts · net gap {formatSignedPercent(cross?.net_difference_fraction)} of gross</small></div>
      </section>

      <Section icon={<BarChart3 size={17} />} title={`${data.underlyer} ${name.toLowerCase()} exposure by strike`}
        detail={`${SCOPE_LABELS[data.scope]} · ${WEIGHT_LABELS[data.weight]} · ${data.units?.[unit === 'usd' ? 'value' : 'shares'] ?? ''} · strikes within ${bandLabel} of spot (${strikeRows.length} of ${data.strikes?.length ?? 0})`}
        aside={<div className="ov-section__tools">
          <Segmented label="Strike window" value={String(band)} onChange={value => setBandChoice(value === 'null' ? null : Number(value))}
            options={BANDS.map(item => ({ value: String(item.value), label: item.label }))} /></div>}>
        <StrikeChart rows={strikeRows} view={view} unit={unit} spot={spot} levels={levels} greek={data.greek} split={split} />
        <div className="ov-chart-foot">
          <span>Levels are gamma levels on the same contracts and weight in every view: flip = where net gamma changes sign (spot moved, IVs held); walls = strikes with the most call or put gamma.</span>
          <button type="button" className="ov-link" aria-expanded={showTable} onClick={() => setShowTable(value => !value)}>{showTable ? 'Hide table' : 'Show as table'}</button>
        </div>
        {showTable && <StrikeTable rows={strikeRows} unit={unit} spot={spot} />}
      </Section>

      <Section icon={<LayoutGrid size={17} />} title={`${data.underlyer} ${name.toLowerCase()} by strike and expiry`}
        detail={`Each cell sums the contracts at one strike and expiry · colour scale ends at the 98th percentile of shown cells · ${unit === 'usd' ? 'dollars' : 'shares'}`}
        aside={<Segmented label="Cells" value={heatmapMode} onChange={setHeatmapMode}
          options={[{ value: 'net', label: 'Net' }, { value: 'call', label: 'Calls' }, { value: 'put', label: 'Puts' }]} />}>
        <Heatmap data={data} band={band} mode={heatmapMode} unit={unit} spot={spot} />
      </Section>

      <div className="flow-profile-grid ov-lower">
        <Section icon={<ListOrdered size={17} />} title="Largest levels" detail={`Strikes ranked by size of net ${name.toLowerCase()} · ${SCOPE_LABELS[data.scope]}`}>
          <div className="flow-table-wrap">
            <table className="flow-table ov-levels">
              <thead><tr><th>Strike</th><th>Net</th><th>Calls</th><th>Puts</th><th>Share</th><th>From spot</th><th>Level</th></tr></thead>
              <tbody>{(data.largest_levels ?? []).map(row => (
                <tr key={row.strike}>
                  <td><strong>{formatStrike(row.strike)}</strong></td>
                  <td className={row.net >= 0 ? 'ov-text-pos' : 'ov-text-neg'}><strong>{formatExposure(row.net, unit, spot)}</strong></td>
                  <td>{formatExposure(row.call, unit, spot)}</td><td>{formatExposure(row.put, unit, spot)}</td>
                  <td title="Share of the range's gross exposure at this strike">{formatPercent(row.share)}</td>
                  <td>{formatSignedPercent(row.distance_to_spot_fraction, 2)}</td>
                  <td>{row.tags.map(tag => <span key={tag} className="ov-tag">{tag === 'CALL_WALL' || tag === 'PUT_WALL'
                    ? <LevelTerm kind={tag === 'CALL_WALL' ? 'call_wall' : 'put_wall'}>{TAG_LABELS[tag]}</LevelTerm>
                    : TAG_LABELS[tag] ?? tag}</span>)}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        </Section>
        <Section icon={<CalendarRange size={17} />} title="Exposure by expiry" detail={`Net ${name.toLowerCase()} per expiry · share of the range's gross exposure`}>
          <Legend view={view} greek={data.greek} netLabel="Net per expiry" />
          <ExpiryStrip rows={data.expiries ?? []} view={view} unit={unit} spot={spot} />
        </Section>
      </div>

      <section className="flow-section ov-notes" role="note" aria-label="How to read this page">
        <header className="flow-section__header"><div><Info size={17} /><div><h2>How to read this page</h2><p>Definitions, coverage and limits</p></div></div></header>
        <ul>
          <li><strong>Research observation, not a trade signal.</strong> Nothing on this page places, suggests or ranks an order.</li>
          <li><strong>Dealer side is assumed:</strong> dealers long calls and short puts. Open interest does not say who holds which side; under the opposite convention every sign flips. Walls, shares and the strike profile’s shape do not depend on it.</li>
          {data.greek === 'gamma'
            ? <li><strong>Gamma exposure</strong> = gamma × weight × 100 × spot² × 1%: dollars of delta dealers would buy or sell for a 1% move (shares = dollars ÷ spot). Positive: hedging leans against moves; negative: hedging follows them.</li>
            : <li><strong>Delta exposure</strong> = delta × weight × 100 × spot: dealers’ option delta in dollars (shares = dollars ÷ spot). Under the convention, long calls and short puts both carry long delta, so both sides are positive; a hedged dealer holds that many shares short.</li>}
          <li><strong>Open interest is the previous settlement.</strong> Today’s trading shows only in the OI + volume weight, which assumes every contract traded today opened a position on the conventional side: an estimate.</li>
          <li><strong>Coverage is the captured band</strong>, not the full listed chain: strikes {formatStrike(coverage.strike_low ?? 0)}–{formatStrike(coverage.strike_high ?? 0)} and expiries {formatDay(coverage.expiry_first)} – {formatDay(coverage.expiry_last)}, refreshed every 5 minutes. Far strikes and longer expiries are not in these totals. {formatCount(coverage.skipped.zero_weight)} contracts in range carry no weight; {formatCount(coverage.skipped.no_greek)} have no IV (quotes below intrinsic); {formatPercent(coverage.weight_coverage_fraction)} of the weight has a Greek.</li>
          <li><strong>Greeks are ours:</strong> Black–Scholes on our implied volatility, the capture’s spot, rate and dividend yield, to each expiry’s close. The provider’s Greeks on the same contracts are the cross-check above.</li>
          {indexName && <li><strong>{data.underlyer} is the ETF, not the {indexName}.</strong> Its options, open interest and strikes are {data.underlyer}’s own, and the levels are {data.underlyer} prices.</li>}
          {notes.filter(note => note.tone === 'note').map(note => <li key={note.text}>{note.text}</li>)}
        </ul>
      </section>
    </div>
  )
}
