/**
 * Live metric fields of an arena entry, from a running query or active merge.
 * Same mapping as the arena's main poll, used by the detail panel's 1s
 * refresh of the selected block so both write identical values.
 */

import type { ActiveMergeInfo, RunningQueryInfo } from '@tracehouse/core';
import type { BlockEntry } from './arena-types';

export type LiveMetrics = Pick<BlockEntry,
  'cpu' | 'mem' | 'elapsed' | 'progress' | 'ioReadRate' | 'rowsRead' | 'bytesRead'
  | 'profileEvents' | 'readBytesPerSec' | 'writeBytesPerSec'>;

/** How often the selected running block is refreshed while the panel is open. */
export const SELECTED_LIVE_REFRESH_MS = 1000;

export function liveMetricsFromQuery(q: RunningQueryInfo): LiveMetrics {
  return {
    cpu: q.cpuCores, mem: q.memoryUsage, elapsed: q.elapsed, progress: q.progress,
    ioReadRate: q.ioReadRate, rowsRead: q.rowsRead, bytesRead: q.bytesRead,
    profileEvents: q.profileEvents,
    readBytesPerSec: undefined, writeBytesPerSec: undefined,
  };
}

export function liveMetricsFromMerge(m: ActiveMergeInfo): LiveMetrics {
  return {
    // system.merges has no CPU: 0 here; heightCpu() gives the block its height.
    cpu: m.cpuEstimate, mem: m.memoryUsage, elapsed: m.elapsed, progress: m.progress,
    ioReadRate: m.readBytesPerSec, rowsRead: m.rowsRead, bytesRead: 0,
    profileEvents: undefined,
    readBytesPerSec: m.readBytesPerSec, writeBytesPerSec: m.writeBytesPerSec,
  };
}
