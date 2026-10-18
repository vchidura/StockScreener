import { useCallback, useRef } from 'react'
import { useSearchParams } from 'react-router-dom'
import type { FormingChartPattern } from '../../services/api'
import {
  CHART_INTERVALS, PATTERN_INTERVAL_ORDER, defaultPeriodForInterval,
  parsePatternChoice, patternChoiceValue, supportsPatternInterval,
} from './tickerShared'

export const CHART_PERIODS = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y'] as const

export function readChartPreferences(params: URLSearchParams) {
  const interval: string = CHART_INTERVALS.find(value => value === params.get('interval')) ?? '1d'
  const period: string = CHART_PERIODS.find(value => value === params.get('period')) ?? defaultPeriodForInterval(interval)
  const analysisPin = params.get('analysis_interval')
  const pattern = params.get('pattern')
  return {
    interval,
    period,
    analysisPin: analysisPin && PATTERN_INTERVAL_ORDER.some(value => value === analysisPin) ? analysisPin : null,
    showRsi: params.get('rsi') === 'on',
    showAutoPatterns: params.get('patterns') === 'on',
    showPriceChannel: params.get('channel') === 'on' && interval !== '1m',
    patternSelection: pattern === 'all' ? 'all'
      : pattern && pattern !== 'best' && supportsPatternInterval(interval) ? patternChoiceValue(interval, pattern) : 'best',
  }
}

export type ChartPreferences = ReturnType<typeof readChartPreferences>

export function useChartPreferences() {
  const [params, setParams] = useSearchParams()
  const search = params.toString()
  const searchState = useRef({ observed: search, pending: params, writes: new Set<string>() })
  if (searchState.current.observed !== search) {
    searchState.current.observed = search
    // An intermediate router commit must not discard later edits in the same burst.
    if (!searchState.current.writes.has(search) || searchState.current.pending.toString() === search) {
      searchState.current.pending = params
      searchState.current.writes.clear()
    }
  }
  const updateChartPreferences = useCallback((changes: Partial<ChartPreferences>) => {
    setParams(() => {
      const next = writeChartPreferences(searchState.current.pending, changes)
      searchState.current.pending = next
      searchState.current.writes.add(next.toString())
      return next
    }, { replace: true })
  }, [setParams])
  return { ...readChartPreferences(params), updateChartPreferences }
}

export function writeChartPreferences(previous: URLSearchParams, changes: Partial<ChartPreferences>) {
  const next = new URLSearchParams(previous)
  const state = { ...readChartPreferences(previous), ...changes }
  next.set('interval', state.interval)
  next.set('period', state.period)
  next.set('patterns', state.showAutoPatterns ? 'on' : 'off')
  next.set('channel', state.showPriceChannel && state.interval !== '1m' ? 'on' : 'off')
  next.set('rsi', state.showRsi ? 'on' : 'off')
  if (state.analysisPin) next.set('analysis_interval', state.analysisPin)
  else next.delete('analysis_interval')
  const choice = parsePatternChoice(state.patternSelection)
  if (state.patternSelection === 'all') next.set('pattern', 'all')
  else if (choice) next.set('pattern', choice.type)
  else next.delete('pattern')
  return next
}

export function selectChartPatterns(patterns: FormingChartPattern[], interval: string, selection: string) {
  if (selection === 'all') return patterns
  if (selection === 'best') return patterns.slice(0, 1)
  const choice = parsePatternChoice(selection)
  const selected = choice?.interval === interval ? patterns.find(pattern => pattern.type === choice.type) : undefined
  return selected ? [selected] : []
}
