export type ChartMetric = 'duration' | 'cpu' | 'memory' | 'rows' | 'status';

export const METRIC_COLORS: Record<ChartMetric, string> = {
  duration: '#58a6ff',
  cpu: '#f59e0b',
  memory: '#10b981',
  rows: '#3b82f6',
  status: '#f85149',
};

export const QUERY_KIND_COLORS: Record<string, string> = {
  SELECT: '#3b82f6', INSERT: '#f59e0b', ALTER: '#ef4444',
  CREATE: '#22c55e', DROP: '#f43f5e', SYSTEM: '#8b5cf6', OPTIMIZE: '#06b6d4',
};

export const percentile = (sortedArr: number[], p: number): number => {
  if (sortedArr.length === 0) return 0;
  if (sortedArr.length === 1) return sortedArr[0];
  const index = (p / 100) * (sortedArr.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sortedArr[lower];
  return sortedArr[lower] + (sortedArr[upper] - sortedArr[lower]) * (index - lower);
};
