import { css } from '@emotion/css';
import { useState, type ReactNode } from 'react';
import type { DetailLinks, DetailSection, ObjectDetail } from '@tracehouse/core/services/object-browser';
import { ModalWrapper } from '../../components/shared/ModalWrapper';
import { SqlHighlight } from '../common/SqlHighlight';
import { ObjectTypeIcon } from './ObjectCard';
import { useDetailStatements, type StatementResult } from './useDetailStatements';

const styles = css`
  /* The modal box clips its content; the detail scrolls inside it, under the sticky header. */
  flex: 1; min-height: 0; overflow-y: auto;
  color: var(--text-primary);
  .detail-header { position: sticky; top: 0; z-index: 2; display: flex; justify-content: space-between; align-items: start; gap: 16px; padding: 16px 24px; background: var(--bg-secondary); border-bottom: 1px solid var(--border-primary); }
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
  .actions { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin-top: 12px; }
  .action { height: 30px; padding: 0 12px; font-size: 12px; color: var(--text-primary); }
  .action:hover { border-color: var(--border-accent); background: var(--bg-hover); }
  .action-user { display: flex; align-items: center; background: var(--bg-tertiary); border: 1px solid var(--border-primary); border-radius: 6px; overflow: hidden; margin-right: 4px; }
  .detail-body section + section { margin-top: 18px; padding-top: 2px; border-top: 1px solid var(--border-secondary); }
  .relation-group + .relation-group { margin-top: 12px; }
  h4 { margin: 0 0 6px; font-size: 12px; font-weight: 600; color: var(--text-secondary); }
  dd.relation-items { font-family: inherit; line-height: 1.7; }
  .relation-item { padding: 0; border: none; background: none; font: inherit; font-size: 12px; color: var(--text-primary); cursor: pointer; }
  .relation-item:hover { color: var(--accent-blue, #58a6ff); text-decoration: underline; }
  .sep { color: var(--text-muted); }
  .detail-body h3 { font-size: 11px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; color: var(--text-muted); }
  details summary { cursor: pointer; font-size: 12px; color: var(--text-secondary); margin: 6px 0; }
`;

type SectionContext = { results: Record<string, StatementResult>; requested: ReadonlySet<string>; load: (sql: string) => void; title: string;
  /** Opens the detail of another object. */
  select: (key: string) => void };
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
      {!result ? waiting : 'error' in result ? <p className="error">Unavailable: {result.error}</p>
        : section.statement.highlight && result.text ? <SqlHighlight>{result.text}</SqlHighlight> : <pre>{result.text || '(empty)'}</pre>}</>;
  },
  relationships: (section, { select }) => <><h3>{section.title}</h3>
    {section.groups.map(group => <div key={group.title} className="relation-group">
      <h4>{group.title}</h4>
      <dl>{group.rows.map(row => <div key={row.relation} style={{ display: 'contents' }}>
        <dt>{row.relation}</dt>
        <dd className="relation-items">{row.items.map((item, i) => <span key={item.key}>
          {i > 0 && <span className="sep" aria-hidden="true"> · </span>}
          <button type="button" className="relation-item" onClick={() => select(item.key)}
            title={[item.fullName, item.method].filter(Boolean).join('\n')}>{item.name}</button>
        </span>)}</dd>
      </div>)}</dl>
    </div>)}</>,
  records: section => <><h3>{section.title}</h3>{section.records.map((record, j) =>
    <details key={j}><summary>{record.host} · system.{record.source}</summary><pre>{JSON.stringify(record.attributes, null, 2)}</pre></details>)}</>,
};
const renderSection = (section: DetailSection, context: SectionContext) =>
  (SECTION_RENDERERS[section.kind] as (s: DetailSection, c: SectionContext) => ReactNode)(section, context);

/** Generic renderer for an object's detail sections; content comes from the core descriptor. */
/** Links to other pages filtered to the object, as one line of text links; links filtered to
 * the user in focus context too get a second line. The tooltip says what each page shows. */
/** Buttons that open other pages filtered to the object, under the title. Buttons filtered to the
 * user in focus context too get their own row. The tooltip says what the page shows. */
function Actions({ links, open }: { links: DetailLinks; open: (url: string) => void }) {
  const [user, setUser] = useState(links.defaultUser);
  const shown = links.byUser[user] ?? links.byUser[''] ?? [];
  if (!shown.length) return null;
  return <div className="actions" role="group" aria-label="Open in">
    {/* One set of pages; the filter decides whose activity they show. */}
    {links.users.length > 0 && <div className="tracehouse-compact-select-control action-user">
      <span className="tracehouse-compact-select-label">User</span>
      <select className="tracehouse-compact-native-select" aria-label="Show activity of" title="Show the activity of one user, or of all users"
        value={user} onChange={e => setUser(e.target.value)}>
        <option value="">All users</option>
        {links.users.map(name => <option key={name} value={name}>{name}</option>)}
      </select>
    </div>}
    {shown.map(link => <button key={link.url} type="button" className="action" title={link.description} aria-label={link.description}
      onClick={() => open(link.url)}>{link.label}</button>)}
  </div>;
}

/** `open` navigates to another page from a link in the header. */
export function ObjectDetailModal({ detail, onClose, open, select }: { detail: ObjectDetail | null; onClose: () => void; open: (url: string) => void;
  select: (key: string) => void }) {
  const statements = detail?.sections.flatMap(s => s.kind === 'statement' ? [s.statement] : []) ?? [];
  const { results, requested, load } = useDetailStatements(statements);
  const card = detail?.card;
  return <ModalWrapper isOpen={!!detail} onClose={onClose} ariaLabel="Object detail" maxWidth={900} backdropBackground="rgba(0, 0, 0, 0.3)" backdropBlur={0}>
    {detail && card && <div className={styles}>
      <div className="detail-header">
        <div><ObjectTypeIcon card={card} />{card.qualifier && <small>{card.qualifier}</small>}<h2>{card.title}</h2><small>{card.label}{card.hosts && ` · ${card.hosts}`}</small>
          {/* Keyed by object so the user filter starts from its default for each object. */}
          <Actions key={`${card.qualifier ?? ''}.${card.title}`} links={detail.links} open={open} /></div>
        <button className="modal-close" aria-label="Close" title="Close (Esc)" onClick={onClose}>✕</button>
      </div>
      <div className="detail-body">{detail.sections.map((section, i) => <section key={i}>{renderSection(section, { results, requested, load, title: card.title, select })}</section>)}</div>
    </div>}
  </ModalWrapper>;
}
