import { useEffect, useMemo, useState } from 'react';
import type { EventSourceCoverage, OperationalEvent } from '@tracehouse/core';
import { clampToAllowed, useRefreshConfig } from '@tracehouse/ui-shared';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import { useGlobalLastUpdatedStore, useRefreshSettingsStore } from '../../stores/refreshSettingsStore';
import { ARENA_EVENT_HORIZON_SEC } from './arena-events';

interface EventSnapshot {
  services: unknown;
  scope: string;
  events: OperationalEvent[];
  coverage: EventSourceCoverage[];
  arrivals: ReadonlyMap<string, number>;
  error: string | null;
}
const EMPTY: OperationalEvent[] = [];
const NO_COVERAGE: EventSourceCoverage[] = [];
const NO_ARRIVALS: ReadonlyMap<string, number> = new Map();

export function useReducedArenaMotion() {
  const [reduced, setReduced] = useState(() => typeof matchMedia !== 'undefined' && matchMedia('(prefers-reduced-motion: reduce)').matches);
  useEffect(() => {
    if (typeof matchMedia === 'undefined') return;
    const query = matchMedia('(prefers-reduced-motion: reduce)');
    const changed = () => setReduced(query.matches);
    query.addEventListener('change', changed);
    return () => query.removeEventListener('change', changed);
  }, []);
  return reduced;
}

/** One shared-service fetch per arena, never per block or animation frame. */
export function useArenaEvents(enabled: boolean, hosts: readonly string[]) {
  const services = useClickHouseServices();
  const capabilities = useMonitoringCapabilitiesStore(s => s.capabilities);
  const refresh = useRefreshSettingsStore(s => s.refreshRateSeconds);
  const refreshConfig = useRefreshConfig();
  const intervalMs = refresh > 0 ? Math.max(10000, clampToAllowed(refresh, refreshConfig) * 1000) : 0;
  const manualRefresh = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const available = useMemo(() => capabilities?.capabilities.filter(c => c.available).map(c => c.id), [capabilities]);
  const scope = JSON.stringify([...hosts].sort());
  const [snapshot, setSnapshot] = useState<EventSnapshot | null>(null);
  useEffect(() => {
    if (!enabled || !services || !available) return;
    let cancelled = false, fetching = false, initialized = false;
    const seen = new Map<string, number>();
    const arrivals = new Map<string, number>();
    const tick = async () => {
      if (fetching || document.hidden) return;
      fetching = true;
      const now = Date.now();
      try {
        const result = await services.eventsService.getEvents({
          startTime: new Date(now - ARENA_EVENT_HORIZON_SEC * 1000).toISOString(),
          endTime: new Date(now).toISOString(), hostname: JSON.parse(scope),
          availableCapabilities: available, origin: 'overview', limit: 1000,
        });
        if (cancelled) return;
        for (const event of result.events) {
          // Initial history is quiet. A late log row pulses on arrival, not as
          // a claim that its event occurred at the time of this fetch.
          if (initialized && !seen.has(event.id)) arrivals.set(event.id, Date.now());
          seen.set(event.id, Date.now());
        }
        // A temporarily missing source or truncated result must not replay an
        // old observation when it returns on the next refresh.
        for (const [id, lastSeen] of seen) if (lastSeen < now - ARENA_EVENT_HORIZON_SEC * 1000) { seen.delete(id); arrivals.delete(id); }
        initialized = true;
        setSnapshot({ services, scope, ...result, arrivals: new Map(arrivals), error: null });
      } catch (error) {
        if (!cancelled) setSnapshot({ services, scope, events: [], coverage: [], arrivals: new Map(),
          error: error instanceof Error ? error.message : String(error) });
      } finally { fetching = false; }
    };
    void tick();
    const timer = intervalMs > 0 ? setInterval(() => { void tick(); }, intervalMs) : undefined;
    const onVisible = () => { if (!document.hidden && intervalMs > 0) void tick(); };
    document.addEventListener('visibilitychange', onVisible);
    return () => { cancelled = true; clearInterval(timer); document.removeEventListener('visibilitychange', onVisible); };
  }, [services, enabled, available, intervalMs, manualRefresh, scope]);
  const current = enabled && snapshot?.services === services && snapshot.scope === scope ? snapshot : null;
  return { events: current?.events ?? EMPTY, coverage: current?.coverage ?? NO_COVERAGE,
    arrivals: current?.arrivals ?? NO_ARRIVALS, error: current?.error ?? null };
}
