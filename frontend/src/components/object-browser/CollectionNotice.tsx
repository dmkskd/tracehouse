import { useState } from 'react';
import { css } from '@emotion/css';
import type { Coverage, Source } from '@tracehouse/core/services/object-browser';

const styles = css`
  margin: 12px 0; padding: 10px 14px; border-left: 3px solid var(--accent-yellow, #d29922);
  background: var(--bg-secondary); color: var(--text-secondary); font-size: 12px;
  .collection-heading { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
  .collection-heading strong { color: var(--text-primary); font-weight: 500; }
  .collection-heading button, .collection-copy { padding: 3px 9px; font-size: 12px; flex-shrink: 0; }
  p { margin: 5px 0; }
  summary { width: fit-content; cursor: pointer; padding: 4px 0; color: var(--text-primary); }
  h3 { margin: 12px 0 4px; font-size: 12px; font-weight: 600; }
  ul { margin: 4px 0 10px; padding-left: 20px; }
  li { margin: 4px 0; overflow-wrap: anywhere; }
  pre { max-height: 280px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere;
    font: 11px/1.5 monospace; color: var(--text-secondary); background: var(--bg-primary); padding: 10px; }
  .collection-copy-state { margin-left: 8px; }
`;

const SOURCE_LABELS: Record<Source, string> = {
  users: 'Users', roles: 'Roles', role_grants: 'Role assignments', grants: 'Grants',
  tables: 'Tables and views', row_policies: 'Row policies',
};

/** Recognize server error identifiers, not numeric memory measurements or arbitrary error prose. */
function cause(reason: string): string {
  if (/MEMORY_LIMIT_EXCEEDED/i.test(reason)) return 'ClickHouse server memory limit exceeded';
  if (/ACCESS_DENIED|not enough privileges/i.test(reason)) return 'Insufficient ClickHouse privileges';
  if (/TIMEOUT_EXCEEDED|ETIMEDOUT|timed?\s*out/i.test(reason)) return 'Collection timed out';
  return 'Other collection limitations';
}

function limitation(entry: Coverage): string {
  if (entry.state === 'failed') return 'Source could not be read.';
  if (entry.state === 'truncated') return 'Row limit reached; additional records were not collected.';
  if (entry.state === 'schema-unavailable') return 'Required source fields were unavailable.';
  if (entry.state === 'not-granted') return 'SELECT was not granted for this source.';
  if (entry.source !== 'tables') return 'Additional source metadata was unavailable.';
  const details: string[] = [];
  const definitions = entry.reason?.match(/Definition references failed for (\d+) object\(s\)/);
  if (definitions) details.push(`Definition analysis failed for ${definitions[1]} object(s).`);
  if (entry.reason?.includes('System tables read by views unavailable')) details.push('Referenced system tables could not be read.');
  if (entry.reason?.includes('link omitted')) details.push('Some dependency links were omitted.');
  if (entry.reason?.includes('Part counts unavailable')) details.push('Part counts could not be collected.');
  if (entry.reason?.includes('Storage policies unavailable')) details.push('Storage policy relationships could not be collected.');
  if (entry.reason?.includes('Disk buckets unavailable')) details.push('Disk bucket metadata could not be collected.');
  return details.join(' ') || 'Some table metadata or relationships could not be collected.';
}

/** Presentation only: preserve complete diagnostics without altering coverage or access evaluation. */
export function CollectionNotice({ coverage = [], error, disabled, onRetry }: {
  coverage?: readonly Coverage[]; error?: string | null; disabled: boolean; onRetry: () => void;
}) {
  const [copyState, setCopyState] = useState('');
  const entries = coverage.filter(entry => entry.state !== 'readable' || entry.reason);
  if (!entries.length && !error) return null;
  const groups = new Map<string, Coverage[]>();
  for (const entry of entries) {
    const label = cause(entry.reason ?? '');
    groups.set(label, [...groups.get(label) ?? [], entry]);
  }
  const onlyTableDetails = !error && entries.every(entry => entry.source === 'tables' && entry.state === 'readable');
  const explanation = error ? 'Collection could not be completed.' : onlyTableDetails
    ? 'Collected objects are shown, but some table metadata or dependency links may be missing.'
    : 'Collected objects are shown, but some source records or metadata may be missing. Access answers report their own collection gaps.';
  const memoryFailure = entries.some(entry => /MEMORY_LIMIT_EXCEEDED/i.test(entry.reason ?? '')) || /MEMORY_LIMIT_EXCEEDED/i.test(error ?? '');
  const technical = [error ? `Collection failed\n${error}` : '', ...entries.map(entry =>
    `${SOURCE_LABELS[entry.source]} (${entry.state})${entry.hosts.length ? `\nObserved hosts: ${entry.hosts.join(', ')}` : ''}\n${entry.reason ?? limitation(entry)}`)].filter(Boolean).join('\n\n');
  const copy = async () => {
    try { await navigator.clipboard.writeText(technical); setCopyState('Copied'); }
    catch { setCopyState('Could not copy. Select the technical text below to copy it.'); }
  };
  return <section className={styles} aria-label="Collection notice">
    <div className="collection-heading">
      <strong role="status">{error ? 'Collection failed' : 'Capture incomplete'}</strong>
      <button type="button" disabled={disabled} onClick={onRetry}>Retry collection</button>
    </div>
    <p>{memoryFailure && 'ClickHouse’s server memory limit was exceeded. '}{explanation}</p>
    <details>
      <summary>Show details</summary>
      {error && <p>{cause(error)}</p>}
      {[...groups].map(([label, affected]) => <div key={label}>
        <h3>{label}</h3>
        <ul>{affected.map((entry, index) => <li key={`${entry.source}-${index}`}>
          <strong>{SOURCE_LABELS[entry.source]}:</strong> {limitation(entry)}
          {entry.hosts.length > 0 && <> Observed hosts: {entry.hosts.join(', ')}.</>}
        </li>)}</ul>
      </div>)}
      <details>
        <summary>Technical errors and coverage</summary>
        <button type="button" className="collection-copy" onClick={() => void copy()}>Copy technical details</button>
        <span className="collection-copy-state" role="status">{copyState}</span>
        <pre>{technical}</pre>
      </details>
    </details>
  </section>;
}
