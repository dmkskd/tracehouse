import { forwardRef, type CSSProperties } from 'react';
import { FLAG_LABELS, type ObjectCard, type ValueRange } from '@tracehouse/core/services/object-browser';
import type { ObjectDiagramStyle } from '../../stores/userPreferenceStore';
import { formatAge, formatBytes, formatNumberCompact } from '../../utils/formatters';
import { OBJECT_SYMBOLS, toneStyle } from './objectStyle';
import { hoverCard } from './objectBrowserStyles';

/** Type text accompanies the symbol so color is never the only distinction. */
export function ObjectTypeIcon({ card }: { card: ObjectCard }) {
  return <span className="object-symbol" style={toneStyle(card.type)} title={card.label} aria-hidden="true">{OBJECT_SYMBOLS[card.type]}</span>;
}

/** Card text: the name and one summary line. The type's color is `--tone`; each diagram
 * style places the dot and summary in its own way. The hover card (ObjectHoverCard) adds the rest. */
/** `includes` replaces the summary line with the selected access types a broad privilege includes. */
export function ObjectCardBody({ card, includes }: { card: ObjectCard; includes?: readonly string[] }) {
  return <span className="node-copy">
    <span className="type-label" aria-hidden="true">{card.label}</span>
    <strong><span className="type-dot" aria-hidden="true" />{card.title}</strong>
    <small>{includes ? <span className="node-includes" title={`Includes ${includes.join(', ')}`}>Includes {includes.join(', ')}</span> : card.summary}{!includes && card.hosts && ` · ${card.hosts}`}{card.flags.map(flag => <span key={flag} className="badge warn">{FLAG_LABELS[flag]}</span>)}</small>
  </span>;
}

const range = ({ min, max }: ValueRange, format: (value: number) => string) =>
  min === max ? format(min) : `${format(min)}-${format(max)}`;

/** Floating summary shown while the pointer rests on a card; its frame follows the diagram style. */
export const ObjectHoverCard = forwardRef<HTMLDivElement, { card: ObjectCard; diagramStyle: ObjectDiagramStyle; position: CSSProperties }>(
  function ObjectHoverCard({ card, diagramStyle, position }, ref) {
    const { hover } = card;
    return <div ref={ref} className={hoverCard} data-style={diagramStyle} role="tooltip" style={{ ...toneStyle(card.type), ...position }}>
      <div className="head"><span className="kind">{hover.type}</span>
        {hover.context && <span className="context">{hover.context.label && <span>{hover.context.label}</span>}<code>{hover.context.value}</code></span>}</div>
      <div className="name">{hover.title}</div>
      {hover.engine && <div className="engine">{hover.engine}</div>}
      {(hover.rows || hover.bytes || hover.parts) && <div className="stats">
        {hover.rows && <span><b>{range(hover.rows, formatNumberCompact)}</b> {hover.rows.max === 1 ? 'row' : 'rows'}</span>}
        {hover.bytes && <span><b>{range(hover.bytes, formatBytes)}</b> on disk</span>}
        {hover.parts && <span><b>{range(hover.parts, String)}</b> active {hover.parts.max === 1 ? 'part' : 'parts'}</span>}
        {hover.newestPart && <span>newest part <b>{formatAge(hover.newestPart)}</b> ago</span>}
      </div>}
      {hover.definition && <div className="definition"><span>{hover.definition.label}</span><code>{hover.definition.text}</code></div>}
      {hover.comment && <p className="comment">{hover.comment}</p>}
      {hover.notes.length > 0 && <div className="notes">{hover.notes.map(note => <span key={note}>{note}</span>)}</div>}
    </div>;
  });
