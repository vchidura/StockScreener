import assert from 'node:assert/strict'
import { existsSync, readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const root = new URL('../../', import.meta.url)
const read = path => readFileSync(new URL(path, root), 'utf8')
const parse = path => ts.createSourceFile(path, read(path), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX)
const collect = (source, predicate) => {
  const matches = []
  const visit = node => {
    if (predicate(node)) matches.push(node)
    ts.forEachChild(node, visit)
  }
  visit(source)
  return matches
}

test('Persistence is standalone and retired routes redirect without importing Stock Research', () => {
  const source = parse('frontend/src/App.tsx')
  const routes = collect(source, node => ts.isJsxSelfClosingElement(node) && node.tagName.getText(source) === 'Route')
  const pathOf = node => node.attributes.properties.find(attribute => attribute.name?.getText(source) === 'path')?.initializer?.text
  assert.ok(routes.some(route => pathOf(route) === '/stocks/persistence' && route.getText(source).includes('<PersistencePage')))
  for (const path of ['/stock-research/*', '/scanner-results', '/scanner-evaluation', '/backtest']) {
    assert.ok(routes.find(route => pathOf(route) === path)?.getText(source).includes('<Navigate to="/stocks/persistence" replace'))
  }
  assert.ok(!read('frontend/src/App.tsx').includes('ScannerResults'))
  assert.match(read('frontend/src/layout/navigation.ts'), /to: '\/stocks\/persistence', label: 'Persistence'/)
  assert.ok(!read('frontend/src/layout/navigation.ts').includes("label: 'Stock Research'"))
})

test('Dashboard and ticker no longer request scanner history; sector performance remains', () => {
  for (const path of ['frontend/src/pages/Dashboard.tsx', 'frontend/src/pages/TickerDetail.tsx', 'frontend/src/services/api.ts']) {
    const source = read(path)
    for (const retired of ['getScannerEventSummary', 'getScannerEventBacklog', 'getScannerEvents', 'getTickerScannerEvents']) {
      assert.ok(!source.includes(retired), `${path}: ${retired}`)
    }
  }
  const ticker = read('frontend/src/pages/ticker/TickerWorkspace.tsx')
  assert.ok(!ticker.includes("label: 'Scanner history'"))
  assert.match(ticker, /if \(view === 'scanner'\) return <Navigate/)
  assert.ok(read('frontend/src/services/api.ts').includes("'/scanner-events/sector-performance'"))
  assert.ok(read('frontend/src/pages/research/PersistenceView.tsx').includes('scanStreak(item.value, PUBLISHED_DAYS)'))
})

test('working pages remain without scanner history or an archive dependency', () => {
  for (const page of ['Dashboard.tsx', 'TickerDetail.tsx']) {
    const original = `frontend/src/pages/${page}`
    assert.ok(existsSync(new URL(original, root)))
    assert.ok(!read(original).includes('scanner-events'))
    assert.ok(!read(original).includes('legacy/'))
  }
})