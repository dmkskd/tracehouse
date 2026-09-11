/**
 * Shared constants and types for the Time Travel timeline feature.
 */
import type { QuerySeries, MergeSeries, MutationSeries, OperationKind } from '@tracehouse/core';
import { formatBytes, formatMicroseconds } from '../../utils/formatters';

export type MetricMode = 'memory' | 'cpu' | 'network' | 'disk';

export type HighlightedItem = { type: 'query' | 'merge' | 'mutation'; idx: number; id: string } | null;

export const Q_COLORS = [
  '#58a6ff', '#3fb950', '#bc8cff', '#79c0ff', '#7ee787', '#d2a8ff', '#a5d6ff', '#56d364',
  '#388bfd', '#2ea043', '#8b5cf6', '#6cb6ff', '#4ade80', '#a78bfa', '#4493f8', '#34d058',
];
export const M_COLORS = [
  '#f0883e', '#e3b341', '#f78166', '#d29922', '#da3633', '#db6d28', '#ffa657', '#f85149',
  '#f0883e', '#e3b341', '#f78166', '#d29922', '#da3633', '#db6d28', '#ffa657', '#f85149',
];
export const MUT_COLORS = [
  '#f778ba', '#ff7eb6', '#ee5396', '#d02670', '#ffafd2', '#ff7eb6', '#f778ba', '#ee5396',
  '#f778ba', '#ff7eb6', '#ee5396', '#d02670', '#ffafd2', '#ff7eb6', '#f778ba', '#ee5396',
];

export const METRIC_CONFIG: Record<MetricMode, { label: string; color: string; fmtVal: (v: number) => string }> = {
  cpu: { label: 'CPU', color: '#3fb950', fmtVal: formatMicroseconds },
  memory: { label: 'Memory', color: '#58a6ff', fmtVal: formatBytes },
  disk: { label: 'Disk I/O', color: '#bc8cff', fmtVal: formatBytes },
  network: { label: 'Network', color: '#d29922', fmtVal: formatBytes },
};

/** Bar chart config for the rich tooltip — shows all 4 resource dimensions at once */
export const METRIC_BAR_CONFIG = [
  { key: 'cpu' as const, label: 'CPU', color: '#3fb950', getValue: (item: QuerySeries | MergeSeries | MutationSeries) => item.cpu_us, fmt: formatMicroseconds },
  { key: 'memory' as const, label: 'MEM', color: '#58a6ff', getValue: (item: QuerySeries | MergeSeries | MutationSeries) => item.peak_memory, fmt: formatBytes },
  { key: 'disk' as const, label: 'DISK', color: '#bc8cff', getValue: (item: QuerySeries | MergeSeries | MutationSeries) => item.disk_read + item.disk_write, fmt: formatBytes },
  { key: 'network' as const, label: 'NET', color: '#d29922', getValue: (item: QuerySeries | MergeSeries | MutationSeries) => item.net_send + item.net_recv, fmt: formatBytes },
] as const;

/** Chart band color for an operation, matching how TimelineChart indexes its palettes. */
export function operationBandColor(kind: OperationKind, idx: number): string {
  const palette = kind === 'query' ? Q_COLORS : kind === 'merge' ? M_COLORS : MUT_COLORS;
  return palette[idx % palette.length];
}

/** Badge colors per query kind, shared by the operations table and the inspector. */
export const QUERY_KIND_COLORS: Record<string, string> = {
  SELECT: '#3b82f6',
  INSERT: '#8b5cf6',
  ALTER: '#ef4444',
  CREATE: '#22c55e',
  DROP: '#f43f5e',
  SYSTEM: '#a78bfa',
  OPTIMIZE: '#06b6d4',
};

/** Color for one breakdown segment: query kinds by name, merges and mutations by kind. */
export function segmentColor(key: string, kind: OperationKind): string {
  if (kind === 'merge') return M_COLORS[0];
  if (kind === 'mutation') return MUT_COLORS[0];
  return QUERY_KIND_COLORS[key] ?? Q_COLORS[0];
}
