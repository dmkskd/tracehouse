import { useId } from 'react';
import { ResourceInfoPopover } from './ResourceInfoPopover';

export function ResourceFailureLegend({ count }: { count: number }) {
  const id = useId();
  if (!count) return null;
  return <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: 'var(--text-secondary)', fontSize: 11 }}>
    <span aria-hidden="true" style={{ display: 'inline-block', width: 18, height: 10, borderRadius: 2,
      background: 'repeating-linear-gradient(125deg, transparent 0 4px, #e06c65 4px 5px, transparent 5px 9px)', border: '1px solid var(--border-primary)' }} />
    <span>Failed operations · {count.toLocaleString()}</span>
    <ResourceInfoPopover id={id} label="Failed operations explanation" title="Striped areas indicate resource usage attributed to failed operations">
      Striped areas indicate resource usage attributed to operations that ended with an error.
    </ResourceInfoPopover>
  </div>;
}
