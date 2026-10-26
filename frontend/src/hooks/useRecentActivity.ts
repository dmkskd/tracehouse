import { useCallback, useEffect, useRef, useState } from 'react';
import { OverviewService, type RecentActivity } from '@tracehouse/core';
import { useClickHouseServices } from '../providers/ClickHouseProvider';

/**
 * Refetch delays after `requestRefresh()`. query_log / part_log rows are
 * written on the system log flush interval (7.5s by default): the first
 * refetch waits past it, and a trailing one, re-armed by every request,
 * catches slow flushes and anything that finished after the first.
 */
const FIRST_REFRESH_MS = 8_000;
const TRAILING_REFRESH_MS = 20_000;

/**
 * Fetch queries and merges that finished within the last `windowSeconds`,
 * once per connection, and again on `requestRefresh()`. Used to backfill the
 * resource arena timeline so a freshly loaded page shows past activity, and to
 * replace the last-poll values of operations seen live with their final ones
 * once they finish.
 *
 * The result is tagged with the services object it was fetched for, so data
 * from a previous connection is never returned after a connection switch.
 */
export function useRecentActivity(
  windowSeconds: number,
  minDurationMs: number,
  enabled: boolean,
): { activity: RecentActivity | null; requestRefresh: () => void } {
  const services = useClickHouseServices();
  const [fetched, setFetched] = useState<{
    services: unknown;
    windowSeconds: number;
    minDurationMs: number;
    activity: RecentActivity;
  } | null>(null);
  const [refreshGen, setRefreshGen] = useState(0);
  const firstTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const trailingTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (!services || !enabled) return;

    let cancelled = false;
    const overviewService = new OverviewService(services.adapter, {}, services.environmentDetector);
    overviewService.getRecentActivity(windowSeconds, { minDurationMs })
      .then(activity => {
        if (!cancelled) setFetched({ services, windowSeconds, minDurationMs, activity });
      })
      .catch(error => {
        console.error('[useRecentActivity] backfill failed:', error);
      });
    return () => { cancelled = true; };
  }, [services, enabled, windowSeconds, minDurationMs, refreshGen]);

  // Completions close together share the first refetch (it is not pushed back,
  // so steady activity still refetches every ~8s); the trailing refetch is
  // re-armed by each request, so the latest completion always gets one at
  // least TRAILING_REFRESH_MS after it.
  const requestRefresh = useCallback(() => {
    if (!firstTimerRef.current) {
      firstTimerRef.current = setTimeout(() => {
        firstTimerRef.current = null;
        setRefreshGen(g => g + 1);
      }, FIRST_REFRESH_MS);
    }
    if (trailingTimerRef.current) clearTimeout(trailingTimerRef.current);
    trailingTimerRef.current = setTimeout(() => {
      trailingTimerRef.current = null;
      setRefreshGen(g => g + 1);
    }, TRAILING_REFRESH_MS);
  }, []);

  useEffect(() => () => {
    if (firstTimerRef.current) clearTimeout(firstTimerRef.current);
    if (trailingTimerRef.current) clearTimeout(trailingTimerRef.current);
    firstTimerRef.current = null;
    trailingTimerRef.current = null;
  }, [services]);

  const current = fetched
    && fetched.services === services
    && fetched.windowSeconds === windowSeconds
    && fetched.minDurationMs === minDurationMs;
  return { activity: enabled && current ? fetched.activity : null, requestRefresh };
}
