import type { TimelineAttribution } from '@tracehouse/core';

/** Zero-width flows cannot be drawn; keep their operation membership visible without inventing usage. */
export function ZeroResourceActivity({ attribution, formattedZero }: { attribution: TimelineAttribution; formattedZero: string }) {
  const rows = attribution.operations.filter(row => row.value === 0 && row.basis !== 'unavailable');
  const groups = [...new Set(rows.map(row => row.activity))].map(activity => {
    const members = rows.filter(row => row.activity === activity);
    const basis = members.every(row => row.basis === 'sampled') ? 'sampled' : members.every(row => row.basis === 'average') ? 'estimated' : 'mixed';
    return { activity, count: members.length, basis };
  });
  if (!groups.length) return null;
  return <div role="note" aria-label="Zero resource contributions" style={{ marginTop: 8, fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.6 }}>
    <div>{attribution.selection.kind === 'instant' ? 'Active with zero attributed usage' : 'Zero attributed work in this range'}</div>
    {groups.map(group => <div key={group.activity}>{group.activity} · {group.count} {group.count === 1 ? 'operation' : 'operations'} · {formattedZero} ({group.basis})</div>)}
  </div>;
}
