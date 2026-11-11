import { useEffect, useMemo, useState } from 'react';
import { resolveDashboardWindow, type EventSourceCoverage, type OperationalEvent, type TimeWindow } from '@tracehouse/core';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import { resolveCustomTimeRange } from '../../utils/customTimeRange';

export interface DashboardEvents {
  /** The dashboard window; time-axis charts use it as their x range so panels line up. */
  window: TimeWindow | null;
  events: OperationalEvent[];
  coverage: EventSourceCoverage[];
  error: string | null;
}

interface FetchResult {
  window: TimeWindow;
  events: OperationalEvent[];
  coverage: EventSourceCoverage[];
  error: string | null;
}

/**
 * The dashboard window, and one events fetch per dashboard shared by every panel.
 * Events are fetched only while `fetchEvents` is on (the dashboard Events toggle).
 */
export function useDashboardEvents(timeRange: string | null, active: boolean, fetchEvents: boolean): DashboardEvents {
  const services = useClickHouseServices();
  const monitoringCapabilities = useMonitoringCapabilitiesStore(state => state.capabilities);
  const availableCapabilities = useMemo(
    () => monitoringCapabilities?.capabilities
      .filter(capability => capability.available)
      .map(capability => capability.id),
    [monitoringCapabilities],
  );
  // A relative window ends when the time range is chosen or the dashboard opens.
  const window = useMemo(
    () => active
      ? resolveDashboardWindow({ interval: timeRange, absolute: resolveCustomTimeRange(timeRange) })
      : null,
    [timeRange, active],
  );
  const [result, setResult] = useState<FetchResult | null>(null);

  useEffect(() => {
    if (!fetchEvents || !window || !services || !availableCapabilities) return;
    let cancelled = false;
    services.eventsService.getEvents({
      startTime: new Date(window.startMs).toISOString(),
      endTime: new Date(window.endMs).toISOString(),
      availableCapabilities,
      origin: 'analytics',
    }).then(
      fetched => { if (!cancelled) setResult({ window, events: fetched.events, coverage: fetched.coverage, error: null }); },
      (error: unknown) => {
        if (!cancelled) setResult({ window, events: [], coverage: [], error: error instanceof Error ? error.message : String(error) });
      },
    );
    return () => { cancelled = true; };
  }, [services, availableCapabilities, window, fetchEvents]);

  // Results for an earlier window are not shown against the current one.
  const current = fetchEvents && result && result.window === window ? result : null;
  return {
    window,
    events: current?.events ?? [],
    coverage: current?.coverage ?? [],
    error: current?.error ?? null,
  };
}
