import { Fragment, type ReactNode } from 'react'
import { money, plainPct } from './tickerShared'

export interface LadderBadge {
  text: string
  tone: string
  title?: string
}

export interface LadderRow {
  key: string
  price: number
  priceLabel?: string
  title: string
  titleTone?: string
  detail?: string
  note?: ReactNode
  badges?: LadderBadge[]
  emphasis?: boolean
}

/** Price-ordered levels with the current price cut in at its true position. */
export default function PriceLadder({ rows, price, priceCaption, emptyMessage, onSelect, selectedKey }: {
  rows: LadderRow[]
  price: number
  priceCaption: string
  emptyMessage: string
  onSelect?: (row: LadderRow) => void
  selectedKey?: string | null
}) {
  if (rows.length === 0) {
    return <p className="tk-ladder__empty">{emptyMessage}</p>
  }

  const ordered = [...rows].sort((left, right) => right.price - left.price)
  const markerIndex = ordered.findIndex(row => row.price < price)
  const marker = (
    <div className="tk-ladder__marker">
      <span>{priceCaption}</span>
    </div>
  )

  return (
    <div className="tk-ladder">
      {ordered.map((row, index) => {
        const distance = price > 0 ? ((row.price - price) / price) * 100 : 0
        const selected = selectedKey === row.key
        const className = [
          'tk-ladder__row',
          row.emphasis ? 'is-emphasis' : '',
          selected ? 'is-selected' : '',
          onSelect ? 'is-clickable' : '',
        ].filter(Boolean).join(' ')

        const content = (
          <>
            <div className="tk-ladder__price">
              <strong>{row.priceLabel ?? money(row.price)}</strong>
              <span>
                {Math.abs(distance) < 0.05
                  ? 'at price'
                  : `${plainPct(Math.abs(distance), 2)} ${distance > 0 ? 'above' : 'below'}`}
              </span>
            </div>
            <div className="tk-ladder__body">
              <div className="tk-ladder__title-row">
                <div className="tk-ladder__title" style={row.titleTone ? { color: row.titleTone } : undefined}>
                  {row.title}
                </div>
                {selected && <span className="tk-ladder__selected-label">On chart</span>}
              </div>
              {row.detail && <div className="tk-ladder__detail">{row.detail}</div>}
              {row.badges?.length ? (
                <div className="tk-ladder__badges">
                  {row.badges.map((badge, badgeIndex) => (
                    <span
                      key={`${badge.text}-${badgeIndex}`}
                      title={badge.title}
                      style={{ color: badge.tone, borderColor: badge.tone }}
                    >
                      {badge.text}
                    </span>
                  ))}
                </div>
              ) : null}
              {row.note && <div className="tk-ladder__note">{row.note}</div>}
            </div>
          </>
        )

        return (
          <Fragment key={row.key}>
            {index === markerIndex && marker}
            {onSelect ? (
              <button
                type="button"
                className={className}
                aria-pressed={selected}
                aria-label={`${selected ? 'Remove' : 'Draw'} ${row.title} at ${row.priceLabel ?? money(row.price)} ${selected ? 'from' : 'on'} the chart`}
                title={selected ? 'Click again to remove this level from the chart' : 'Draw this level on the chart'}
                onClick={() => onSelect(row)}
              >
                {content}
              </button>
            ) : (
              <div className={className}>{content}</div>
            )}
          </Fragment>
        )
      })}
      {markerIndex === -1 && marker}
    </div>
  )
}
