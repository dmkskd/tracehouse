import { useMemo } from 'react';
import type { SummaryDirectives } from './metaLanguage';
import { buildResultSummary } from './resultSummary';

/** Result-wide metrics; independent of chart type and domain-specific columns. */
export function ChartSummary({ rows, config, unavailableReason }: {
  rows: Record<string, unknown>[];
  config?: SummaryDirectives;
  unavailableReason?: string;
}) {
  const summary = useMemo(() => buildResultSummary(rows, config), [rows, config]);
  if (!config || !rows.length) return null;
  if (unavailableReason) return <div style={{ fontSize: 11, color: 'var(--text-muted)', paddingBottom: 6 }}>{unavailableReason}</div>;
  return <>
    {summary.values.length > 0 && <div aria-label="Result summary" style={{ display: 'flex', flexWrap: 'wrap', gap: '8px 24px', padding: '4px 0 10px', fontSize: 12, color: 'var(--text-muted)', flexShrink: 0 }}>
      {summary.values.map((metric, index) => <span key={index}>
        {metric.label}{' '}<strong aria-label={metric.accessibleLabel} title={metric.accessibleLabel} style={{ color: 'var(--text-primary)', fontWeight: 600, overflowWrap: 'anywhere' }}>{metric.display}</strong>
      </span>)}
    </div>}
    {summary.diagnostics.length > 0 && <details style={{ fontSize: 11, color: 'var(--text-muted)', paddingBottom: 6 }}>
      <summary>Summary unavailable</summary>
      <ul>{summary.diagnostics.map((message, index) => <li key={index}>{message}</li>)}</ul>
    </details>}
  </>;
}
