import { css } from '@emotion/css';
import type { ReactNode } from 'react';
import type { DetailSection, ObjectDetail } from '@tracehouse/core/services/object-browser';
import { ModalWrapper } from '../../components/shared/ModalWrapper';
import { ObjectTypeIcon } from './ObjectCard';
import { useDetailStatements, type StatementResult } from './useDetailStatements';

const styles = css`
  /* The modal box clips its content; the detail scrolls inside it, under the sticky header. */
  flex: 1; min-height: 0; overflow-y: auto;
  color: var(--text-primary);
  .detail-header { position: sticky; top: 0; z-index: 1; display: flex; justify-content: space-between; align-items: start; gap: 16px; padding: 16px 24px; background: var(--bg-card); border-bottom: 1px solid var(--border-primary); }
  .detail-header small { display: block; font-size: 11px; color: var(--text-muted); }
  .detail-header h2 { margin: 2px 0; font-size: 17px; overflow-wrap: break-word; }
  .detail-body { padding: 8px 24px 24px; }
  h3 { font-size: 12px; margin: 18px 0 8px; color: var(--text-secondary); }
  button { background: var(--bg-secondary); color: var(--text-primary); border: 1px solid var(--border-primary); border-radius: 5px; padding: 7px 11px; cursor: pointer; }
  .load-external { font-size: 12px; padding: 5px 10px; }
  .modal-close { border: 0; background: none; padding: 4px 6px; font-size: 14px; color: var(--text-muted); }
  dl { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 5px 16px; margin: 0; font-size: 12px; }
  dt { color: var(--text-muted); }
  dd { margin: 0; font-family: monospace; overflow-wrap: anywhere; }
  pre { margin: 0; font: 11px/1.65 monospace; white-space: pre-wrap; overflow-wrap: anywhere; color: var(--text-secondary); background: var(--bg-secondary); padding: 12px; border-radius: 4px; }
  .muted { font-size: 12px; color: var(--text-muted); }
  .error { font-size: 12px; color: var(--accent-yellow, #ba913f); }
  ul { margin: 0; padding: 0; list-style: none; font-size: 12px; line-height: 1.9; }
  .method { display: block; font-size: 10px; color: var(--text-muted); line-height: 1.4; margin-bottom: 4px; }
  details summary { cursor: pointer; font-size: 12px; color: var(--text-secondary); margin: 6px 0; }
`;

type SectionContext = { results: Record<string, StatementResult>; requested: ReadonlySet<string>; load: (sql: string) => void; title: string };
type Renderers = { [K in DetailSection['kind']]: (section: Extract<DetailSection, { kind: K }>, context: SectionContext) => ReactNode };
/** One renderer per section kind; the core descriptor decides which sections an object has. */
const SECTION_RENDERERS: Renderers = {
  facts: section => <><h3>{section.title}</h3>
    <dl>{section.facts.map((f, i) => <div key={i} style={{ display: 'contents' }}><dt>{f.label}</dt><dd>{f.value}</dd></div>)}</dl></>,
  statement: (section, { results, requested, load }) => {
    const { sql, title, external } = section.statement;
    const result = results[sql];
    const waiting = external && !requested.has(sql)
      ? <button className="load-external" onClick={() => load(sql)}>Load · {external}</button> : <p className="muted">Loading…</p>;
    return <><h3>{title}</h3>
      {!result ? waiting : 'error' in result ? <p className="error">Unavailable: {result.error}</p> : <pre>{result.text || '(empty)'}</pre>}</>;
  },
  relationships: (section, { title }) => <><h3>{section.title}</h3>
    <ul>{section.relationships.map((r, j) => <li key={j}>{r.direction === 'out' ? `${r.relation} → ${r.name}` : `${r.name} → ${r.relation} → ${title}`}
      {r.method && <span className="method">{r.method}</span>}</li>)}</ul></>,
  records: section => <><h3>{section.title}</h3>{section.records.map((record, j) =>
    <details key={j}><summary>{record.host} · system.{record.source}</summary><pre>{JSON.stringify(record.attributes, null, 2)}</pre></details>)}</>,
};
const renderSection = (section: DetailSection, context: SectionContext) =>
  (SECTION_RENDERERS[section.kind] as (s: DetailSection, c: SectionContext) => ReactNode)(section, context);

/** Generic renderer for an object's detail sections; content comes from the core descriptor. */
export function ObjectDetailModal({ detail, onClose }: { detail: ObjectDetail | null; onClose: () => void }) {
  const statements = detail?.sections.flatMap(s => s.kind === 'statement' ? [s.statement] : []) ?? [];
  const { results, requested, load } = useDetailStatements(statements);
  const card = detail?.card;
  return <ModalWrapper isOpen={!!detail} onClose={onClose} ariaLabel="Object detail" maxWidth={900} backdropBackground="rgba(0, 0, 0, 0.3)" backdropBlur={0}>
    {detail && card && <div className={styles}>
      <div className="detail-header">
        <div><ObjectTypeIcon card={card} />{card.qualifier && <small>{card.qualifier}</small>}<h2>{card.title}</h2><small>{card.label}{card.hosts && ` · ${card.hosts}`}</small></div>
        <button className="modal-close" aria-label="Close" title="Close (Esc)" onClick={onClose}>✕</button>
      </div>
      <div className="detail-body">{detail.sections.map((section, i) => <section key={i}>{renderSection(section, { results, requested, load, title: card.title })}</section>)}</div>
    </div>}
  </ModalWrapper>;
}
