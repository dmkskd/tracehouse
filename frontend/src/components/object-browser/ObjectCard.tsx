import type { ObjectCard } from '@tracehouse/core/services/object-browser';
import { OBJECT_SYMBOLS, toneStyle } from './objectStyle';

/** Type text accompanies the symbol so color is never the only distinction. */
export function ObjectTypeIcon({ card }: { card: ObjectCard }) {
  return <span className="object-symbol" style={toneStyle(card.type)} title={card.label} aria-hidden="true">{OBJECT_SYMBOLS[card.type]}</span>;
}

const FLAG_TEXT: Record<ObjectCard['flags'][number], string> = {
  differs: 'differs across hosts', partial: 'partial / not comparable', unresolved: 'unresolved reference',
  'outside-catalog': 'outside catalog',
};

/** Card text: the name and one summary line. The type's color is `--tone`; each diagram
 * style places the dot and summary in its own way. Every fact is in the hover text. */
export function ObjectCardBody({ card }: { card: ObjectCard }) {
  const hover = [card.title, card.summary, ...card.facts.map(f => `${f.label} ${f.value}`), card.hosts].filter(Boolean).join('\n');
  return <span className="node-copy" title={hover}>
    <span className="type-label" aria-hidden="true">{card.label}</span>
    <strong><span className="type-dot" aria-hidden="true" />{card.title}</strong>
    <small>{card.summary}{card.hosts && ` · ${card.hosts}`}{card.flags.map(flag => <span key={flag} className="badge warn">{FLAG_TEXT[flag]}</span>)}</small>
  </span>;
}
