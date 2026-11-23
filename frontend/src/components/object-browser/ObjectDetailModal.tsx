import { css } from '@emotion/css';
import { useState, type ReactNode } from 'react';
import type { DetailLinks, DetailSection, ObjectDetail, TableActivity, TablePlacement } from '@tracehouse/core/services/object-browser';
import { formatBytes } from '../../utils/formatters';
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
  .stat-tiles { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; margin-bottom: 12px; }
  .stat-tile { display: flex; flex-direction: column; align-items: flex-start; gap: 2px; padding: 10px 12px; border: 1px solid var(--border-secondary); border-radius: 6px; background: var(--bg-secondary); text-align: left; font: inherit; color: var(--text-primary); }
  button.stat-tile { cursor: pointer; }
  button.stat-tile:hover { border-color: var(--border-accent); background: var(--bg-hover); }
  .stat-value { font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums; line-height: 1.2; }
  .stat-label { font-size: 11px; color: var(--text-muted); }
  .stat-tile.zero .stat-value { color: var(--text-muted); }
  .stat-tile.failed .stat-value { color: var(--accent-red, #e5534b); }
  .stat-lines dd { font-family: inherit; }
  .stat-count { font-variant-numeric: tabular-nums; color: var(--text-muted); }
  .lifecycle-steps { margin: 0 0 12px; padding-left: 18px; font-size: 12px; line-height: 1.8; }
  .lifecycle-steps .when { color: var(--text-muted); margin-right: 8px; }
  .placement { width: 100%; border-collapse: collapse; font-size: 12px; font-variant-numeric: tabular-nums; }
  .placement th { text-align: left; font-weight: 500; color: var(--text-muted); padding: 4px 12px 4px 0; border-bottom: 1px solid var(--border-secondary); }
  .placement td { padding: 4px 12px 4px 0; }
  .lifecycle-settings { margin-bottom: 12px; }
  details summary { cursor: pointer; font-size: 12px; color: var(--text-secondary); margin: 6px 0; }
`;

const number = (value: number) => value.toLocaleString('en-US');
/** Query counts as tiles, then when the last query ran and who queried most. Each count,
 * user and the last query opens the Queries page on it. */
function Activity({ activity, open }: { activity: TableActivity | null; open: (url: string) => void }) {
  if (!activity) return <p className="muted">No queries in the last 24 hours.</p>;
  return <>
    <div className="stat-tiles">{activity.counts.map(({ label, value, description, url }) => {
      const className = `stat-tile${value === 0 ? ' zero' : ''}${label === 'Failed' && value > 0 ? ' failed' : ''}`;
      const body = <><span className="stat-value">{number(value)}</span><span className="stat-label">{label}</span></>;
      return value === 0 ? <div key={label} className={className} title={description}>{body}</div>
        : <button key={label} type="button" className={className} title={description} onClick={() => open(url)}>{body}</button>;
    })}</div>
    <dl className="stat-lines">
      <dt>Last query</dt><dd><button type="button" className="relation-item" title={`${activity.lastQuery} (server time)`}
        onClick={() => open(activity.lastQueryUrl)}>{activity.lastQueryAgo}</button></dd>
      <dt>{activity.users === 1 ? 'User' : `Users (${number(activity.users)})`}</dt>
      <dd className="stat-users">{activity.topUsers.map(({ user, queries, url }, i) => <span key={user}>
        {i > 0 && <span className="sep" aria-hidden="true"> · </span>}
        <button type="button" className="relation-item" title={`Queries by ${user}`} onClick={() => open(url)}>{user}</button>{' '}
        <span className="stat-count">{number(queries)}</span></span>)}
        {activity.users > activity.topUsers.length && <span className="muted"> · {number(activity.users - activity.topUsers.length)} more</span>}</dd>
    </dl>
  </>;
}

type SectionContext = { results: Record<string, StatementResult>; requested: ReadonlySet<string>; load: (sql: string) => void; title: string;
  /** Navigates to another page. */
  open: (url: string) => void;
  /** Opens the detail of another object. */
  select: (key: string) => void };
type Renderers = { [K in DetailSection['kind']]: (section: Extract<DetailSection, { kind: K }>, context: SectionContext) => ReactNode };
/** Active parts per disk on the connected host, with move TTL state. */
function Placement({ rows }: { rows: TablePlacement[] }) {
  if (!rows.length) return <p className="muted">No active parts.</p>;
  return <table className="placement"><thead><tr><th>Disk</th><th>Storage</th><th>Parts</th><th>Rows</th><th>Size</th><th>Past move TTL</th><th>Next move</th></tr></thead>
    <tbody>{rows.map(row => <tr key={row.disk}><td>{row.disk}</td><td>{row.storage}</td><td>{number(row.parts)}</td><td>{number(row.rows)}</td>
      <td>{formatBytes(row.bytes)}</td><td>{row.pastMoveTTL ? `${number(row.pastMoveTTL)} parts` : ''}</td><td>{row.nextMove ?? ''}</td></tr>)}</tbody></table>;
}
/** One renderer per section kind; the core descriptor decides which sections an object has. */
const SECTION_RENDERERS: Renderers = {
  facts: section => <><h3>{section.title}</h3>
    <dl>{section.facts.map((f, i) => <div key={i} style={{ display: 'contents' }}><dt>{f.label}</dt><dd>{f.value}</dd></div>)}</dl></>,
  activity: (section, { results, open }) => {
    const result = results[section.statement.sql];
    return <><h3>{section.title}</h3>
      {!result ? <p className="muted">Loading…</p> : 'error' in result ? <p className="error">Unavailable: {result.error}</p>
        : 'activity' in result ? <Activity activity={result.activity} open={open} /> : null}</>;
  },
  statement: (section, { results, requested, load }) => {
    const { sql, title, external } = section.statement;
    const result = results[sql];
    const waiting = external && !requested.has(sql)
      ? <button className="load-external" onClick={() => load(sql)}>Load · {external}</button> : <p className="muted">Loading…</p>;
    return <><h3>{title}</h3>
      {!result ? waiting : 'error' in result ? <p className="error">Unavailable: {result.error}</p>
        : !('text' in result) ? null : section.statement.highlight && result.text ? <SqlHighlight>{result.text}</SqlHighlight> : <pre>{result.text || '(empty)'}</pre>}</>;
  },
  lifecycle: (section, { results, select }) => {
    const result = results[section.statement.sql];
    return <><h3>{section.title}</h3>
      {section.steps.length > 0 && <ol className="lifecycle-steps">{section.steps.map((step, i) => <li key={i}>
        <span className="when">{step.when}</span>{step.action}
        {step.target && <><span className="sep"> · </span><button type="button" className="relation-item" onClick={() => select(step.target!.key)}>{step.target.name}</button></>}
      </li>)}</ol>}
      {section.settings.length > 0 && <dl className="lifecycle-settings">{section.settings.map(f => <div key={f.label} style={{ display: 'contents' }}><dt>{f.label}</dt><dd>{f.value}</dd></div>)}</dl>}
      <h4>Parts on this host</h4>
      {!result ? <p className="muted">Loading…</p> : 'error' in result ? <p className="error">Unavailable: {result.error}</p>
        : 'placement' in result ? <Placement rows={result.placement} /> : null}</>;
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

const NO_SECTIONS: DetailSection[] = [];
/** `open` navigates to another page from a link in the header. */
export function ObjectDetailModal({ detail, onClose, open, select }: { detail: ObjectDetail | null; onClose: () => void; open: (url: string) => void;
  select: (key: string) => void }) {
  const { results, requested, load } = useDetailStatements(detail?.sections ?? NO_SECTIONS);
  const card = detail?.card;
  return <ModalWrapper isOpen={!!detail} onClose={onClose} ariaLabel="Object detail" maxWidth={900} backdropBackground="rgba(0, 0, 0, 0.3)" backdropBlur={0}>
    {detail && card && <div className={styles}>
      <div className="detail-header">
        <div><ObjectTypeIcon card={card} />{card.qualifier && <small>{card.qualifier}</small>}<h2>{card.title}</h2><small>{card.label}{card.hosts && ` · ${card.hosts}`}</small>
          {/* Keyed by object so the user filter starts from its default for each object. */}
          <Actions key={`${card.qualifier ?? ''}.${card.title}`} links={detail.links} open={open} /></div>
        <button className="modal-close" aria-label="Close" title="Close (Esc)" onClick={onClose}>✕</button>
      </div>
      <div className="detail-body">{detail.sections.map((section, i) => <section key={i}>{renderSection(section, { results, requested, load, title: card.title, select, open })}</section>)}</div>
    </div>}
  </ModalWrapper>;
}
