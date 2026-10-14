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
    assert.ok(routes.find(route => pathOf(route) === path)?.getText(source).includes('<PersistenceRedirect'))
  }
  assert.ok(!read('frontend/src/App.tsx').includes('ScannerResults'))
  assert.match(read('frontend/src/layout/navigation.ts'), /to: '\/stocks\/persistence', label: 'Persistence'/)
  assert.ok(!read('frontend/src/layout/navigation.ts').includes("label: 'Stock Research'"))
  assert.match(read('frontend/src/App.tsx'), /Navigate to={`\/stocks\/persistence\$\{location.search\}`}/)
})

test('Dashboard and ticker no longer request scanner history; sector intelligence remains', () => {
  for (const path of ['frontend/src/pages/Dashboard.tsx', 'frontend/src/pages/TickerDetail.tsx', 'frontend/src/services/api.ts']) {
    const source = read(path)
    for (const retired of ['getScannerEventSummary', 'getScannerEventBacklog', 'getScannerEvents', 'getTickerScannerEvents']) {
      assert.ok(!source.includes(retired), `${path}: ${retired}`)
    }
  }
  const tickerSource = parse('frontend/src/pages/ticker/TickerWorkspace.tsx')
  const ticker = tickerSource.text
  assert.ok(!ticker.includes("label: 'Scanner history'"))
  const scannerBranch = collect(tickerSource, node => ts.isIfStatement(node)
    && ts.isBinaryExpression(node.expression)
    && node.expression.operatorToken.kind === ts.SyntaxKind.EqualsEqualsEqualsToken
    && node.expression.left.getText(tickerSource) === 'view'
    && ts.isStringLiteral(node.expression.right) && node.expression.right.text === 'scanner')[0]
  assert.ok(scannerBranch, 'Retired scanner view must have an explicit redirect')
  const redirect = collect(scannerBranch, node => ts.isReturnStatement(node)
    && node.expression && ts.isJsxSelfClosingElement(node.expression)
    && node.expression.tagName.getText(tickerSource) === 'Navigate')[0]?.expression
  assert.ok(redirect, 'Retired scanner view must return Navigate')
  const attributes = redirect.attributes.properties
  const destination = attributes.find(attribute => attribute.name?.getText(tickerSource) === 'to')?.initializer?.expression
  assert.ok(destination && ts.isTemplateExpression(destination))
  assert.equal(destination.head.text, '/ticker/')
  assert.deepEqual(destination.templateSpans.map(span => [span.expression.getText(tickerSource), span.literal.text]),
    [['symbol', ''], ['location.search', '']])
  assert.ok(attributes.some(attribute => attribute.name?.getText(tickerSource) === 'replace' && !attribute.initializer))
  assert.ok(read('frontend/src/services/api.ts').includes("'/sector-intelligence'"))
  assert.ok(read('frontend/src/pages/SectorIntelligence.tsx').includes('getSectorIntelligence(5)'))
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