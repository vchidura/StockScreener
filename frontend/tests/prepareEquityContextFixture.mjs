import { build } from 'esbuild'
import { readFile, mkdir, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

const frontend = fileURLToPath(new URL('../', import.meta.url))
const destination = fileURLToPath(new URL('../../backend/backups/equity-simplification/ui-fixture.html', import.meta.url))
const frame = { status: 'READY', market_time: '2026-09-21T20:00:00+00:00', trend: 'Bullish Stack' }
const data = { schema: 'stock_equity_context_v1', as_of: '2026-09-22T10:00:00+00:00', execution_permission: false,
  cohort: { session: '2026-09-21', expected_members: 400, eligible_ranked: 380, status: 'READY' },
  rows: ['AAPL', 'MSFT', 'NVDA', 'AMD', 'AAA', 'BBB', 'CCC', 'DDD'].map((ticker, index) => ({
    security_id: ticker, ticker, sector: index < 4 ? 'Technology' : 'Industrials',
    daily: { status: 'READY', percentile: index % 2 ? .02 : .98, momentum: index % 2 ? -.12 : .35, state: index % 2 ? 'BOUNCE' : 'PULLBACK' },
    frames: { '1d': frame, '1h': frame, '30m': frame },
  })) }
const entry = `import React from 'react';
import {createRoot} from 'react-dom/client';
import {MemoryRouter} from 'react-router-dom';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {MomentumRanks,SectorEquityContext,TickerEquityContext} from './src/pages/EquityContextPanels';
globalThis.fixtureContext=${JSON.stringify(data)};
const client=new QueryClient({defaultOptions:{queries:{retry:false}}});
createRoot(document.getElementById('root')).render(<MemoryRouter><QueryClientProvider client={client}><main>
<MomentumRanks/><SectorEquityContext sector="Technology"/>
<TickerEquityContext context={{...globalThis.fixtureContext,rows:[globalThis.fixtureContext.rows[0]]}}/>
</main></QueryClientProvider></MemoryRouter>);`
const compiled = await build({ stdin: { contents: entry, loader: 'jsx', resolveDir: frontend },
  bundle: true, write: false, format: 'iife', platform: 'browser', jsx: 'automatic',
  define: { 'process.env.NODE_ENV': '"production"' }, logLevel: 'silent',
  plugins: [{ name: 'fixture-read-only-context', setup(builder) {
    builder.onResolve({ filter: /services\/stockContext$/ }, () => ({ path: 'context', namespace: 'fixture' }))
    builder.onLoad({ filter: /.*/, namespace: 'fixture' }, () => ({ contents: 'export async function getStockContext(){ return globalThis.fixtureContext; }' }))
  } }] })
const css = (await Promise.all(['src/styles/tokens.css', 'src/index.css', 'src/layout/pageChrome.css'].map(name => readFile(path.join(frontend, name), 'utf8')))).join('\n')
await mkdir(path.dirname(destination), { recursive: true })
await writeFile(destination, `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Equity context validation fixture</title><style>${css}\nbody{padding:16px;background:var(--tm-canvas);color:var(--tm-ink)}main{max-width:1100px;margin:auto}</style></head><body><div id="root"></div><script>${compiled.outputFiles[0].text}</script></body></html>`, 'utf8')
console.log(JSON.stringify({ status: 'FIXTURE_BUILT', output: destination, provider_requests: false, api_server: false }))