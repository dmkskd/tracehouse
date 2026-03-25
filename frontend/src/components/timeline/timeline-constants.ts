/**
 * Shared constants and types for the Time Travel timeline feature.
 */
import type { QuerySeries, MergeSeries, MutationSeries } from '@tracehouse/core';
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

/** Format a timeline item's metric value based on the current mode */
export function metricForItem(item: QuerySeries | MergeSeries | MutationSeries, mode: MetricMode): string {
  if (mode === 'memory') return formatBytes(item.peak_memory);
  if (mode === 'cpu') return formatMicroseconds(item.cpu_us);
  if (mode === 'network') return formatBytes(item.net_send + item.net_recv);
  return formatBytes(item.disk_read + item.disk_write);
}

/** Get raw metric value for sorting */
export function getMetricValue(item: QuerySeries | MergeSeries | MutationSeries, mode: MetricMode): number {
  if (mode === 'memory') return item.peak_memory;
  if (mode === 'cpu') return item.cpu_us;
  if (mode === 'network') return item.net_send + item.net_recv;
  return item.disk_read + item.disk_write;
}

/** Bar chart config for the rich tooltip — shows all 4 resource dimensions at once */
export const METRIC_BAR_CONFIG = [
  { key: 'cpu' as const, label: 'CPU', color: '#3fb950', getValue: (item: QuerySeries | MergeSeries | MutationSeries) => item.cpu_us, fmt: formatMicroseconds },
  { key: 'memory' as const, label: 'MEM', color: '#58a6ff', getValue: (item: QuerySeries | MergeSeries | MutationSeries) => item.peak_memory, fmt: formatBytes },
  { key: 'disk' as const, label: 'DISK', color: '#bc8cff', getValue: (item: QuerySeries | MergeSeries | MutationSeries) => item.disk_read + item.disk_write, fmt: formatBytes },
  { key: 'network' as const, label: 'NET', color: '#d29922', getValue: (item: QuerySeries | MergeSeries | MutationSeries) => item.net_send + item.net_recv, fmt: formatBytes },
] as const;
