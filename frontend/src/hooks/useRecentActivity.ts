import { useEffect, useState } from 'react';
import { OverviewService, type RecentActivity } from '@tracehouse/core';
import { useClickHouseServices } from '../providers/ClickHouseProvider';

/**
 * Fetch queries and merges that finished within the last `windowSeconds`,
 * once per connection. Used to backfill the resource arena timeline so a
 * freshly loaded page shows past activity, not only what is running now.
 *
 * The result is tagged with the services object it was fetched for, so data
 * from a previous connection is never returned after a connection switch.
 */
export function useRecentActivity(
  windowSeconds: number,
  minDurationMs: number,
  enabled: boolean,
): RecentActivity | null {
  const services = useClickHouseServices();
  const [fetched, setFetched] = useState<{
    services: unknown;
    windowSeconds: number;
    minDurationMs: number;
    activity: RecentActivity;
  } | null>(null);

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
  }, [services, enabled, windowSeconds, minDurationMs]);

  const current = fetched
    && fetched.services === services
    && fetched.windowSeconds === windowSeconds
    && fetched.minDurationMs === minDurationMs;
  return enabled && current ? fetched.activity : null;
}
