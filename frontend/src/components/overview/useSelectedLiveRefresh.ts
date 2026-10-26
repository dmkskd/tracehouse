/**
 * useSelectedLiveRefresh — while the arena's detail panel is open on a running
 * block, fetch just that block every second, independent of the page's
 * refresh rate (which drives the arena's full poll, and may be paused).
 *
 * One single-row lookup per tick (system.processes by query_id, or
 * system.merges by result part on the block's host), skipped while the
 * previous one is in flight. Returns:
 * - `metrics`: the latest values, to lay over the block in the panel. Dropped
 *   on an error or a lookup stuck past STALE_AFTER_MS, so an old override never
 *   hides newer page samples.
 * - `finishedAt`: set when the lookup no longer finds the operation, so the
 *   arena can mark it finished without waiting for the next page poll (and
 *   copy `metrics` into its entry once).
 * Both are empty once the block is no longer running.
 * Stops when the selection changes or the block finishes.
 */

import { useEffect, useState } from 'react';
import { OverviewService } from '@tracehouse/core';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { liveMetricsFromMerge, liveMetricsFromQuery, SELECTED_LIVE_REFRESH_MS, type LiveMetrics } from './arena-live';
import type { BlockEntry } from './arena-types';

const STALE_AFTER_MS = SELECTED_LIVE_REFRESH_MS * 3;

export interface SelectedLive {
  metrics: LiveMetrics | null;
  finishedAt: number | null;
}

const NONE: SelectedLive = { metrics: null, finishedAt: null };

export function useSelectedLiveRefresh(selected: BlockEntry | null): SelectedLive {
  const services = useClickHouseServices();
  const [live, setLive] = useState<{ id: string; state: SelectedLive } | null>(null);

  const id = selected?.id ?? null;
  const running = selected?.endTime === null;
  const queryId = selected && !selected.isMerge ? selected.queryId : undefined;
  const mergeRef = selected?.isMerge && selected.database && selected.table && selected.partName
    ? { database: selected.database, table: selected.table, partName: selected.partName, hostname: selected.hostname ?? '' }
    : null;
  // Includes the host: replicas merge the same part name independently.
  const mergeKey = mergeRef
    ? [mergeRef.database, mergeRef.table, mergeRef.partName, mergeRef.hostname].join('\u0000')
    : null;

  useEffect(() => {
    if (!services || !id || !running || (!queryId && !mergeKey)) return;
    const overview = new OverviewService(services.adapter, {}, services.environmentDetector);
    const [database, table, partName, hostname] = mergeKey ? mergeKey.split('\u0000') : [];
    let cancelled = false;
    let done = false;
    let inFlightSince: number | null = null;
    let loggedError = false;

    const tick = () => {
      if (done) return;
      if (inFlightSince !== null) {
        // A lookup that hangs must not leave its last values standing in.
        if (Date.now() - inFlightSince > STALE_AFTER_MS) setLive(null);
        return;
      }
      inFlightSince = Date.now();
      const request: Promise<LiveMetrics | null> = queryId
        ? overview.getRunningQuery(queryId).then(q => (q ? liveMetricsFromQuery(q) : null))
        : overview.getActiveMerge(database, table, partName, hostname || undefined).then(m => (m ? liveMetricsFromMerge(m) : null));
      request
        .then(metrics => {
          if (cancelled) return;
          if (metrics) {
            setLive({ id, state: { metrics, finishedAt: null } });
          } else {
            // Gone from system.processes / system.merges: it finished.
            done = true;
            const finishedAt = Date.now();
            setLive(prev => ({ id, state: { metrics: prev?.id === id ? prev.state.metrics : null, finishedAt } }));
          }
        })
        .catch((err: unknown) => {
          if (cancelled) return;
          if (!loggedError) console.error('[useSelectedLiveRefresh] refresh failed:', err);
          loggedError = true;
          setLive(null);
        })
        .finally(() => { inFlightSince = null; });
    };
    const timer = setInterval(tick, SELECTED_LIVE_REFRESH_MS);
    return () => { cancelled = true; clearInterval(timer); };
  }, [services, id, running, queryId, mergeKey]);

  // Only while running: once finished, the registry entry (and then its final
  // record) is the source of truth, and an old sample must not override it.
  return live && live.id === id && running ? live.state : NONE;
}
