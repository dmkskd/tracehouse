/**
 * useTraceSampleCounts — lightweight background probe to discover which seconds
 * of a query have CPU profiler data in trace_log. No introspection functions needed.
 *
 * useTimeScopedFlamegraph — fetches a flamegraph scoped to a specific time
 * window within a query, for display in the X-Ray scrubber popup.
 */

import { useState, useCallback, useMemo } from 'react';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';

/** Background probe: which seconds have profiler samples? */
export interface TraceSampleCountsResult {
  /** Map of t_second → sample count, null if not yet loaded */
  sampleCounts: Map<number, number> | null;
  isLoading: boolean;
  error: string | null;
  fetch: () => Promise<void>;
}

export function useTraceSampleCounts(
  queryId: string | undefined,
  queryStartTime?: string,
  eventDate?: string,
): TraceSampleCountsResult {
  const services = useClickHouseServices();
  const [sampleCounts, setSampleCounts] = useState<Map<number, number> | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetch = useCallback(async () => {
    if (!services || !queryId || !queryStartTime) return;
    setIsLoading(true);
    setError(null);
    try {
      const result = await services.traceService.getTraceSampleCounts(queryId, queryStartTime, eventDate);
      setSampleCounts(result);
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Failed to probe trace_log';
      console.error('[useTraceSampleCounts]', msg, e);
      setError(msg);
    } finally {
      setIsLoading(false);
    }
  }, [services, queryId, queryStartTime, eventDate]);

  return useMemo(() => ({ sampleCounts, isLoading, error, fetch }), [sampleCounts, isLoading, error, fetch]);
}

/**
 * Check if any seconds in [fromT, toT) have profiler samples.
 */
export function hasTraceSamplesInRange(
  sampleCounts: Map<number, number>,
  fromT: number,
  toT: number,
): boolean {
  for (let t = Math.floor(fromT); t < Math.ceil(toT); t++) {
    if ((sampleCounts.get(t) || 0) > 0) return true;
  }
  return false;
}

/** On-demand fetch for the time-scoped flamegraph popup */
export interface TimeScopedFlamegraphResult {
  folded: string;
  unavailableReason?: string;
  isLoading: boolean;
  error: string | null;
  fetch: (queryId: string, fromTime: string, toTime: string, eventDate?: string) => Promise<void>;
  clear: () => void;
}

export function useTimeScopedFlamegraph(): TimeScopedFlamegraphResult {
  const services = useClickHouseServices();
  const [folded, setFolded] = useState('');
  const [unavailableReason, setUnavailableReason] = useState<string | undefined>();
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetch = useCallback(async (queryId: string, fromTime: string, toTime: string, eventDate?: string) => {
    if (!services) return;
    setIsLoading(true);
    setError(null);
    setFolded('');
    setUnavailableReason(undefined);
    try {
      const result = await services.traceService.getFlamegraphFoldedForTimeRange(queryId, fromTime, toTime, eventDate);
      setFolded(result.folded);
      setUnavailableReason(result.unavailableReason);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch flamegraph');
    } finally {
      setIsLoading(false);
    }
  }, [services]);

  const clear = useCallback(() => {
    setFolded('');
    setUnavailableReason(undefined);
    setError(null);
  }, []);

  return { folded, unavailableReason, isLoading, error, fetch, clear };
}
