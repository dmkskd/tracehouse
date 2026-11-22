import { useState } from 'react';
import { css } from '@emotion/css';
import { GUIDES, investigationFor, type Guide, type GraphNode, type Investigation, type ObjectCard, type ObjectGraph } from '@tracehouse/core/services/object-browser';
import { ObjectTypeIcon } from './ObjectCard';

const MAX_CANDIDATES = 12;
const styles = css`
  margin: 0 0 12px;
  .section-toggle { display: flex; align-items: center; gap: 4px; margin: 0 0 6px; padding: 0; border: 0; background: none; cursor: pointer;
    font-size: 10px; font-weight: 600; letter-spacing: .08em; text-transform: uppercase; color: var(--text-muted); }
  .section-toggle:hover { color: var(--text-primary); }
  .section-toggle .chevron { font-size: 14px; line-height: 1; letter-spacing: 0; }
  .guides { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 12px;
    @media (max-width: 1400px) { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    @media (max-width: 700px) { grid-template-columns: minmax(0, 1fr); } }
  .guide { display: flex; flex-direction: column; gap: 3px; min-width: 0; padding: 8px 12px; border-radius: 8px; border: 1px solid var(--border-primary);
    background: var(--bg-card); color: var(--text-primary); text-align: left; cursor: pointer; transition: background .15s ease, border-color .15s ease; }
  .guide:hover, .guide[aria-pressed=true] { background: var(--bg-card-hover); border-color: var(--accent-blue); }
  .guide strong { font-size: 11px; font-weight: 600; color: var(--accent-blue); white-space: nowrap; }
  .guide small { min-width: 0; font: 10px monospace; color: var(--text-muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .guide-question { display: block; font-size: 12.5px; font-weight: 500; line-height: 1.4; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .picker { margin-top: 12px; padding: 14px; border: 1px solid var(--border-primary); border-radius: 8px; background: var(--bg-card); }
  .picker-head { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; font-size: 12px; color: var(--text-secondary); }
  .picker-head input { width: 240px; padding: 6px 9px; border: 1px solid var(--border-primary); border-radius: 5px; background: var(--bg-secondary); color: var(--text-primary); font-size: 12px; }
  .picked { color: var(--text-primary); }
  .candidates { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
  .candidates button { display: flex; align-items: center; padding: 4px 9px; font-size: 12px; border-radius: 5px; }
  .candidates .object-symbol { font-size: 15px; margin-right: 6px; }
  .close { margin-left: auto; border: 0; background: none; color: var(--text-muted); padding: 2px 6px; }
`;

/** Guided questions (GUIDES in core). Each card asks for its picks in order, then
 * focuses the last pick or starts a provenance capture. */
export function ObjectGuides({ graph, cards, onFocus, onInvestigate }: {
  graph: ObjectGraph; cards: Map<string, ObjectCard>;
  onFocus: (key: string) => void; onInvestigate: (input: Omit<Investigation, 'connection'>) => void;
}) {
  const [expanded, setExpanded] = useState(true);
  const [active, setActive] = useState<Guide | null>(null);
  const [picked, setPicked] = useState<GraphNode[]>([]);
  const [filter, setFilter] = useState('');
  const start = (guide: Guide | null) => { setActive(guide); setPicked([]); setFilter(''); };
  const pick = active?.picks[picked.length];
  const candidates = pick ? pick.candidates(graph).filter(n => n.name.toLowerCase().includes(filter.trim().toLowerCase())) : [];
  const choose = (node: GraphNode) => {
    if (!active) return;
    const chosen = [...picked, node];
    if (chosen.length < active.picks.length) { setPicked(chosen); setFilter(''); return; }
    if (active.action === 'investigate') onInvestigate(investigationFor(chosen[0], chosen[1]));
    else onFocus(node.key);
    start(null);
  };
  // The label sits on the row it controls, styled like the diagram's band labels.
  return <section className={styles} aria-label="Explore objects">
    <button className="section-toggle" aria-expanded={expanded} onClick={() => { setExpanded(value => !value); start(null); }}>
      Explore <span className="chevron" aria-hidden="true">{expanded ? '▾' : '▸'}</span></button>
    {expanded && <div className="guides">{GUIDES.map(guide =>
      <button key={guide.id} className="guide" aria-pressed={active?.id === guide.id} onClick={() => start(active?.id === guide.id ? null : guide)}>
        <strong>{guide.title}</strong><span className="guide-question" title={guide.description}>{guide.question}</span><small>{guide.signal(graph)}</small>
      </button>)}
    </div>}
    {expanded && active && pick && <div className="picker" role="group" aria-label={active.question}>
      <div className="picker-head">
        {picked.map(node => <span key={node.key} className="picked">{node.name} ·</span>)}
        <span>{pick.label}</span>
        <input type="search" aria-label={`Find ${pick.label.toLowerCase()}`} placeholder={`Find ${pick.label.toLowerCase()}`} value={filter} onChange={e => setFilter(e.target.value)} autoFocus />
        <span className="muted">{candidates.length} in the catalog</span>
        <button className="close" aria-label="Close question" onClick={() => start(null)}>✕</button>
      </div>
      <div className="candidates">{candidates.slice(0, MAX_CANDIDATES).map(node =>
        <button key={node.key} onClick={() => choose(node)}>{cards.get(node.key) && <ObjectTypeIcon card={cards.get(node.key)!} />}{node.name}</button>)}
        {!candidates.length && <span className="muted">None in this catalog.</span>}
      </div>
    </div>}
  </section>;
}
