// Nov 12 (TP7): hover titles and collapsible legends for the ticker page abbreviations (data in tickerShared).
import type { ReactNode } from 'react'
import { GLOSSARY, glossaryTitle } from './tickerShared'

export { GLOSSARY, TIMEFRAME_TERMS, glossaryTitle } from './tickerShared'

/** An abbreviation with its definition on hover. */
export function Abbr({ term, children }: { term: string; children?: ReactNode }) {
  return <abbr className="tk-abbr" title={glossaryTitle(term)}>{children ?? term}</abbr>
}

/** Small collapsible list of the abbreviations a view uses. */
export function AbbreviationLegend({ terms, label = 'Abbreviations' }: { terms: string[]; label?: string }) {
  return (
    <details className="tk-legend">
      <summary>{label}: {terms.join(' · ')}</summary>
      <dl>
        {terms.filter(term => GLOSSARY[term]).map(term => (
          <div key={term}>
            <dt>{term}</dt>
            <dd><strong>{GLOSSARY[term].name}.</strong> {GLOSSARY[term].meaning}</dd>
          </div>
        ))}
      </dl>
    </details>
  )
}
