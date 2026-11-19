import { css } from '@emotion/css';
import type { TimelineAttribution, ResourceAttributionSummary } from '@tracehouse/core';

const style = css`
  font-size: 11px; color: var(--text-secondary); line-height: 1.6;
  summary { cursor: pointer; padding: 3px 0; }
  summary:focus-visible { outline: 2px solid #58a6ff; outline-offset: 3px; border-radius: 3px; }
  .zero-resource-body { padding: 6px 0 4px 14px; }
  .zero-resource-body p { margin: 0 0 5px; max-width: 440px; }
`;

/** Zero-width flows cannot be drawn; keep their operation membership visible without inventing usage. */
export function ZeroResourceActivity({ attribution, formattedZero, summary }: { attribution: TimelineAttribution; formattedZero: string; summary?: ResourceAttributionSummary }) {
  const rows = attribution.operations.filter(row => row.value === 0 && row.basis !== 'unavailable');
  const groups = [...new Set(rows.map(row => row.activity))].map(activity => {
    const members = rows.filter(row => row.activity === activity);
    const basis = members.every(row => row.basis === 'sampled') ? 'sampled' : members.every(row => row.basis === 'average') ? 'estimated' : 'mixed';
    return { activity, count: members.length, basis };
  });
  if (summary) {
    for (const activity of new Set(summary.groups.filter(group => group.zeroCount > 0).map(group => group.activity))) {
      groups.push({ activity, count: summary.groups.filter(group => group.activity === activity).reduce((sum,group) => sum+group.zeroCount,0), basis: '' });
    }
  }
  if (!groups.length) return null;
  const count = groups.reduce((sum,group) => sum+group.count,0);
  const resource = attribution.mode === 'cpu' ? attribution.selection.kind === 'range' ? 'CPU time' : 'CPU usage'
    : attribution.mode === 'memory' ? 'memory' : attribution.mode === 'disk' ? 'disk I/O' : 'network traffic';
  return <div role="note" aria-label="Zero resource contributions" className={style}>
    <details><summary>0 {resource} · {count.toLocaleString()} {count === 1 ? 'operation' : 'operations'}</summary>
      <div className="zero-resource-body">
        <p>Operations with an attributed {resource} value of 0 {attribution.selection.kind === 'range' ? 'over the selected time range' : 'at the selected timestamp'} are counted but omitted from the flow.</p>
        {groups.map(group => <div key={group.activity}>{group.activity} · {group.count.toLocaleString()} {group.count === 1 ? 'operation' : 'operations'}{attribution.selection.kind === 'instant' ? ` · ${formattedZero}` : ''}{group.basis ? ` (${group.basis})` : ''}</div>)}
      </div>
    </details>
  </div>;
}
