import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import Module from 'node:module'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import test from 'node:test'
import ts from 'typescript'

const require = createRequire(import.meta.url)
const root = fileURLToPath(new URL('../src/', import.meta.url))
const tokens = readFileSync(path.join(root, 'styles', 'tokens.css'), 'utf8')
const dark = tokens.slice(tokens.indexOf(':root {'), tokens.indexOf(":root[data-theme='light']"))
const light = tokens.slice(tokens.indexOf(":root[data-theme='light']"))
const values = block => new Map([...block.matchAll(/(--[\w-]+):\s*([^;]+);/g)].map(match => [match[1], match[2].trim()]))

function loadTheme(react) {
  const filename = path.join(root, 'pages', 'ticker', 'chartTheme.ts')
  const module = new Module(filename)
  module.filename = filename
  module.paths = Module._nodeModulePaths(path.dirname(filename))
  module.require = specifier => specifier === 'react' ? react : require(specifier)
  module._compile(ts.transpileModule(readFileSync(filename, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText, filename)
  return module.exports
}

function luminance(hex) {
  assert.match(hex, /^#[0-9a-f]{6}$/i)
  const channels = [1, 3, 5].map(offset => parseInt(hex.slice(offset, offset + 2), 16) / 255)
    .map(value => value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4)
  return channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722
}

test('every pattern role resolves to a concrete color with at least 3:1 chart contrast in both themes', () => {
  const previous = { document: globalThis.document, getComputedStyle: globalThis.getComputedStyle }
  globalThis.document = { documentElement: {} }
  try {
    for (const [name, block] of [['dark', dark], ['light', light]]) {
      const colors = values(block)
      globalThis.getComputedStyle = () => ({ getPropertyValue: key => colors.get(key) ?? '' })
      const theme = loadTheme({}).resolveChartTheme()
      assert.equal(Object.keys(theme.pattern).length, 8)
      assert.equal(theme.up, colors.get('--tm-chart-up'))
      assert.equal(theme.down, colors.get('--tm-chart-down'))
      assert.equal(theme.pattern.support, colors.get('--tm-info'))
      assert.equal(theme.pattern.resistance, colors.get('--tm-alt'))
      assert.equal(theme.pattern.neckline, colors.get('--tm-warn'))
      for (const [role, color] of Object.entries(theme.pattern)) {
        assert.doesNotMatch(color, /var\(/)
        assert.notEqual(color, theme.up, `${name} ${role} must not reuse the up-candle color`)
        assert.notEqual(color, theme.down, `${name} ${role} must not reuse the down-candle color`)
        const a = luminance(color), b = luminance(theme.background)
        const ratio = (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05)
        assert.ok(ratio >= 3, `${name} ${role}: ${ratio.toFixed(2)}:1`)
      }
    }
  } finally {
    Object.assign(globalThis, previous)
  }
})

test('theme changes resolve new pattern colors and disconnect their observer on unmount', () => {
  const previous = { document: globalThis.document, getComputedStyle: globalThis.getComputedStyle,
    MutationObserver: globalThis.MutationObserver }
  let colors = values(dark), current, callback, cleanup, disconnected = false
  globalThis.document = { documentElement: {} }
  globalThis.getComputedStyle = () => ({ getPropertyValue: key => colors.get(key) ?? '' })
  globalThis.MutationObserver = class {
    constructor(next) { callback = next }
    observe(element, options) { assert.deepEqual(options.attributeFilter, ['data-theme']) }
    disconnect() { disconnected = true }
  }
  try {
    const { useChartTheme } = loadTheme({
      useState: initial => { current ??= initial(); return [current, next => { current = next }] },
      useEffect: effect => { cleanup = effect() },
    })
    const initial = useChartTheme()
    colors = values(light)
    callback()
    assert.notEqual(current.pattern.support, initial.pattern.support)
    assert.equal(current.pattern.support, '#0284c7')
    assert.equal(current.pattern.resistance, '#7c3aed')
    cleanup()
    assert.equal(disconnected, true)
  } finally {
    Object.assign(globalThis, previous)
  }
})

test('pattern and channel effects wait for chart data and redraw when chart data or theme changes', () => {
  const source = readFileSync(path.join(root, 'pages', 'TickerDetail.tsx'), 'utf8')
  assert.match(source, /!patternSupported \|\| finalizedChartData.length === 0/)
  assert.match(source, /\[clearPatternSeries, displayedPatterns, finalizedChartData, showAutoPatterns, patternSupported, chartTheme\]/)
  assert.match(source, /\[clearChannelSeries, priceChannel, finalizedChartData, showPriceChannel, chartTheme\]/)
  assert.match(source, /color: chartTheme.pattern\[line.role\]/)
  assert.match(source, /lineWidth: patternIndex === 0 \? 3 : 2/)
})
