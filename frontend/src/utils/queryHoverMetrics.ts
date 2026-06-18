import type { QueryHistoryItem } from '../stores/queryStore';
import { formatBytes, formatDurationMs } from './formatters';

export type ResourcePressureDimension = 'time' | 'memory' | 'cpu' | 'io' | 'scan';
export type ResourcePressureLevel = 'low' | 'moderate' | 'high';

export interface ResourcePressureScores {
  time: number;
  memory: number;
  cpu: number;
  io: number;
  scan: number;
}

export interface ResourcePressureMetrics {
  scores: ResourcePressureScores;
  level: ResourcePressureLevel;
  cpuMs: number;
  ioBytes: number;
  scanDisplay: string;
}

export interface ScanEfficiencyMetrics {
  readWidth: number;
  resultWidth: number;
  pruningDisplay: string;
  pruningLevel: ResourcePressureLevel | 'none';
  partsPct: number | null;
  marksPct: number | null;
}

export const clampPct = (value: number): number => Math.max(4, Math.min(100, value));

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const normalizeKind = (kind: string): string => kind ? kind.toUpperCase() : 'QUERY';

const isSuccessfulQuery = (query: QueryHistoryItem): boolean =>
  !query.exception && query.type !== 'ExceptionWhileProcessing' && query.type !== 'error';

export const selectedPct = (selected?: number, total?: number): number | null => {
  if (selected == null || total == null || total <= 0) return null;
  return (selected / total) * 100;
};

export const normalizeLog = (value: number, low: number, high: number): number => {
  if (value <= 0) return 0;
  const safeLow = Math.max(1, low);
  const safeHigh = Math.max(safeLow + 1, high);
  return clamp01((Math.log10(value) - Math.log10(safeLow)) / (Math.log10(safeHigh) - Math.log10(safeLow)));
};

export const scanDisplay = (query: Pick<QueryHistoryItem, 'efficiency_score'>): string => {
  if (query.efficiency_score == null) return 'n/a';
  if (query.efficiency_score <= 0) return 'full scan';
  return `${query.efficiency_score.toFixed(1)}% pruned`;
};

export const resourcePressureScores = (query: QueryHistoryItem): ResourcePressureScores => {
  const cpuMs = (query.cpu_time_us ?? 0) / 1000;
  const ioBytes = Math.max(query.read_bytes, query.disk_read_bytes ?? 0, query.network_receive_bytes ?? 0);

  // Fixed anchors are a first-pass heuristic. They will not fit every cluster,
  // workload, or deployment size; replace with dynamic baselines/percentiles.
  return {
    time: normalizeLog(query.query_duration_ms, 100, 60_000),
    memory: normalizeLog(query.memory_usage, 32 * 1024 * 1024, 8 * 1024 * 1024 * 1024),
    cpu: normalizeLog(cpuMs, 100, 60_000),
    io: normalizeLog(ioBytes, 1024 * 1024, 10 * 1024 * 1024 * 1024),
    scan: query.efficiency_score != null ? 1 - (query.efficiency_score / 100) : 0,
  };
};

export const resourcePressureLevel = (
  query: QueryHistoryItem,
  scores: ResourcePressureScores = resourcePressureScores(query),
): ResourcePressureLevel => {
  if (!isSuccessfulQuery(query)) return 'high';
  if (normalizeKind(query.query_kind) === 'INSERT') return 'moderate';

  const resourceValues = [scores.time, scores.memory, scores.cpu, scores.io];
  const resourceMax = Math.max(...resourceValues);
  const elevatedResources = resourceValues.filter(value => value >= 0.65).length;

  if (elevatedResources >= 2 || resourceMax >= 0.9 || (scores.scan >= 0.9 && elevatedResources >= 1)) return 'high';
  if (elevatedResources === 1 || resourceMax >= 0.45 || scores.scan >= 0.75) return 'moderate';
  return 'low';
};

export const buildResourcePressureMetrics = (query: QueryHistoryItem): ResourcePressureMetrics => {
  const scores = resourcePressureScores(query);
  return {
    scores,
    level: resourcePressureLevel(query, scores),
    cpuMs: (query.cpu_time_us ?? 0) / 1000,
    ioBytes: Math.max(query.read_bytes, query.disk_read_bytes ?? 0, query.network_receive_bytes ?? 0),
    scanDisplay: scanDisplay(query),
  };
};

export const resourcePressureTooltip = (query: QueryHistoryItem): string => {
  const metrics = buildResourcePressureMetrics(query);
  return [
    `Resource pressure: ${metrics.level}`,
    'Shape dimensions: time, memory, CPU, I/O, scan',
    `Time: ${formatDurationMs(query.query_duration_ms)}`,
    `Memory: ${formatBytes(query.memory_usage)}`,
    `CPU: ${formatDurationMs(metrics.cpuMs)}`,
    `I/O: ${formatBytes(metrics.ioBytes)}`,
    `Scan: ${metrics.scanDisplay}`,
  ].join('\n');
};

export const buildScanEfficiencyMetrics = (query: QueryHistoryItem): ScanEfficiencyMetrics => {
  const partsPct = selectedPct(query.selected_parts, query.selected_parts_total);
  const marksPct = selectedPct(query.selected_marks, query.selected_marks_total);
  const pruningLevel = query.efficiency_score == null
    ? 'none'
    : query.efficiency_score >= 70
      ? 'low'
      : query.efficiency_score === 0
        ? 'high'
        : 'moderate';

  return {
    readWidth: query.read_bytes > 0 || query.result_bytes > 0 ? 100 : 8,
    resultWidth: query.read_bytes > 0
      ? clampPct((query.result_bytes / Math.max(1, query.read_bytes)) * 100)
      : query.result_bytes > 0 ? 100 : 8,
    pruningDisplay: scanDisplay(query) === 'n/a' ? 'no pruning data' : scanDisplay(query),
    pruningLevel,
    partsPct: partsPct == null ? null : clampPct(partsPct),
    marksPct: marksPct == null ? null : clampPct(marksPct),
  };
};
