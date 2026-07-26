export type GlobalRefreshStatus = 'idle' | 'polling' | 'error';

export function globalRefreshLabel(
  refreshRateSeconds: number,
  lastUpdated: Date | null,
  status: GlobalRefreshStatus,
  nowMs = Date.now(),
): string {
  if (refreshRateSeconds === 0) return 'Paused';
  if (status === 'error') return 'Refresh failed';
  if (!lastUpdated) return 'Ready';

  const secsAgo = Math.round((nowMs - lastUpdated.getTime()) / 1000);
  if (secsAgo < 2) return 'Just now';
  if (secsAgo < 60) return `${secsAgo}s ago`;
  return `${Math.floor(secsAgo / 60)}m ago`;
}
