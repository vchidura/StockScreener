import { useEffect, useState } from 'react'
import type { FormingPatternLine } from '../../services/api'

/** lightweight-charts needs concrete colors, so theme tokens are resolved from the DOM. */
export interface ChartTheme {
  background: string
  text: string
  grid: string
  border: string
  up: string
  down: string
  upFill: string
  downFill: string
  bb: string
  ma: Record<string, string>
  ink: string
  muted: string
  accent: string
  pattern: Record<FormingPatternLine['role'], string>
}

export function resolveChartTheme(): ChartTheme {
  const style = getComputedStyle(document.documentElement)
  const read = (name: string, fallback: string) => style.getPropertyValue(name).trim() || fallback
  const up = read('--tm-chart-up', '#15803d')
  const down = read('--tm-chart-down', '#b91c1c')
  const accent = read('--tm-accent', '#2563eb')
  const muted = read('--tm-muted', '#64748b')
  const warn = read('--tm-warn', '#a16207')
  const patternSupport = read('--tm-info', '#0284c7')
  const patternResistance = read('--tm-alt', '#7c3aed')
  return {
    background: read('--tm-chart-bg', '#ffffff'),
    text: read('--tm-chart-text', '#334155'),
    grid: read('--tm-chart-grid', '#f0f0f0'),
    border: read('--tm-chart-border', '#e0e0e0'),
    up,
    down,
    upFill: read('--tm-chart-up-fill', 'rgba(21, 128, 61, 0.28)'),
    downFill: read('--tm-chart-down-fill', 'rgba(185, 28, 28, 0.28)'),
    bb: read('--tm-chart-bb', '#cbd5e1'),
    ma: {
      'MA 50': read('--tm-chart-ma50', '#475569'),
      'MA 100': read('--tm-chart-ma100', '#7c3aed'),
      'MA 200': read('--tm-chart-ma200', '#0f766e'),
      'EMA 8': read('--tm-chart-ema8', '#ea580c'),
      'EMA 21': read('--tm-chart-ema21', '#2563eb'),
      'EMA 50': read('--tm-chart-ema50', '#be185d'),
    },
    ink: read('--tm-ink', '#0f172a'),
    muted,
    accent,
    pattern: {
      resistance: patternResistance,
      support: patternSupport,
      neckline: warn,
      structure: patternResistance,
      rim: warn,
      cup: patternSupport,
      handle: patternSupport,
      flagpole: muted,
    },
  }
}

export function useChartTheme(): ChartTheme {
  const [theme, setTheme] = useState<ChartTheme>(resolveChartTheme)

  useEffect(() => {
    const observer = new MutationObserver(() => setTheme(resolveChartTheme()))
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] })
    return () => observer.disconnect()
  }, [])

  return theme
}
