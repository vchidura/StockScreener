export type PatternTickerTarget = {
  ticker: string
  interval: string
  patternType: string
  hasChannel: boolean
  snapshotId?: string
}

export function patternTickerPath(target: PatternTickerTarget, search: string): string {
  const params = new URLSearchParams(search)
  params.delete('ticker')
  if (target.snapshotId !== undefined) params.set('snapshot_id', target.snapshotId)
  params.set('interval', target.interval)
  params.set('patterns', 'on')
  params.set('pattern', target.patternType)
  if (target.hasChannel) params.set('channel', 'on')
  else params.delete('channel')
  return `/ticker/${encodeURIComponent(target.ticker)}?${params.toString()}`
}
