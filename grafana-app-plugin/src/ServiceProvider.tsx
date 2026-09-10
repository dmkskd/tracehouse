import React, { createContext, useContext, useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { getDataSourceSrv, locationService } from '@grafana/runtime';
import { dateTime } from '@grafana/data';
import { lastValueFrom } from 'rxjs';
import { GrafanaAdapter, type AdapterFrame, type AdapterQueryFn } from '@tracehouse/core/adapters/grafana-adapter';
import { ClusterAwareAdapter } from '@tracehouse/core/adapters/cluster-adapter';
import { ClusterService } from '@tracehouse/core/services/cluster-service';
import { DatabaseExplorer } from '@tracehouse/core/services/database-explorer';
import { MergeTracker } from '@tracehouse/core/services/merge-tracker';
import { MetricsCollector } from '@tracehouse/core/services/metrics-collector';
import { QueryAnalyzer } from '@tracehouse/core/services/query-analyzer';
import { InteractiveQueryService } from '@tracehouse/core/services/interactive-query-service';
import { QueryExecutionAnalysisService } from '@tracehouse/core/services/query-execution-analysis';
import { ColumnCostService } from '@tracehouse/core/services/column-cost-service';
import {
  TimelineService,
  EventsService,
  EventContextService,
} from '@tracehouse/core';
import { TraceService } from '@tracehouse/core/services/trace-service';
import { AnalyticsService } from '@tracehouse/core/services/analytics-service';
import { ObservabilityMapService } from '@tracehouse/core/services/observability-map-service';
import { MonitoringCapabilitiesService } from '@tracehouse/core/services/monitoring-capabilities';
import { EnvironmentDetector } from '@tracehouse/core/services/environment-detector';
import type { IClickHouseAdapter } from '@tracehouse/core/adapters/types';
import { 
  ClickHouseContext, 
  type ClickHouseServices,
  useClickHouseServices,
} from '@tracehouse/ui-shared/hooks/useClickHouseServices';
import { useConnectionStore } from './stores/connectionStore';
import { useMonitoringCapabilitiesStore } from '@frontend/stores/monitoringCapabilitiesStore';
import { useClusterStore } from '@frontend/stores/clusterStore';
import { usePluginConfig } from './PluginConfigContext';
import {
  readShareCoordinates,
  shareCoordinateUpdate,
} from './shareCoordinates';

// Re-export for convenience
export { useClickHouseServices };
export type { ClickHouseServices };

interface ServiceContextValue {
  services: ClickHouseServices | null;
  datasourceUid: string | null;
  datasourceName: string | null;
  setDatasourceUid: (uid: string, name?: string) => void;
  error: string | null;
  isLoading: boolean;
}

const ServiceContext = createContext<ServiceContextValue | null>(null);

const STORAGE_KEY = 'tracehouse-datasource';
const CLUSTER_STORAGE_KEY = 'tracehouse-cluster';

function listClickHouseDatasources() {
  try {
    return getDataSourceSrv().getList({ type: 'grafana-clickhouse-datasource' });
  } catch {
    return [];
  }
}

function loadClusterOverride(datasourceUid: string): string | null {
  try {
    const stored = localStorage.getItem(CLUSTER_STORAGE_KEY);
    if (stored) {
      const map = JSON.parse(stored);
      return map[datasourceUid] ?? null;
    }
  } catch { /* ignore */ }
  return null;
}

function saveClusterOverride(datasourceUid: string, clusterName: string | null): void {
  try {
    const stored = localStorage.getItem(CLUSTER_STORAGE_KEY);
    const map = stored ? JSON.parse(stored) : {};
    if (clusterName) {
      map[datasourceUid] = clusterName;
    } else {
      delete map[datasourceUid];
    }
    localStorage.setItem(CLUSTER_STORAGE_KEY, JSON.stringify(map));
  } catch { /* ignore */ }
}

export function ServiceProvider({ children }: { children: React.ReactNode }) {
  const sharedCoordinatesRef = useRef(readShareCoordinates(locationService.getLocation().search));
  const restoringSharedClusterRef = useRef(sharedCoordinatesRef.current !== null);
  const [datasourceUid, setDatasourceUidState] = useState<string | null>(() => {
    const requestedUid = sharedCoordinatesRef.current?.datasourceUid;
    if (requestedUid) {
      const requested = listClickHouseDatasources()
        .find(candidate => candidate.uid === requestedUid);
      if (requested) return requested.uid;
      return null;
    }
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const parsed = JSON.parse(stored);
        // Sync with connectionStore on initial load
        if (parsed.uid) {
          setTimeout(() => {
            const connectionStore = useConnectionStore.getState();
            connectionStore._setGrafanaDatasource(parsed.uid, parsed.name || 'ClickHouse');
          }, 0);
        }
        return parsed.uid || null;
      }
    } catch { /* ignore */ }
    return null;
  });
  const [datasourceName, setDatasourceName] = useState<string | null>(() => {
    const requestedUid = sharedCoordinatesRef.current?.datasourceUid;
    if (requestedUid) {
      return listClickHouseDatasources()
        .find(candidate => candidate.uid === requestedUid)?.name ?? null;
    }
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const parsed = JSON.parse(stored);
        return parsed.name || null;
      }
    } catch { /* ignore */ }
    return null;
  });
  const [error, setError] = useState<string | null>(() => {
    const requestedUid = sharedCoordinatesRef.current?.datasourceUid;
    return requestedUid && !listClickHouseDatasources()
      .some(candidate => candidate.uid === requestedUid)
      ? `The shared datasource "${requestedUid}" is unavailable in this Grafana organization.`
      : null;
  });
  const [clusterReady, setClusterReady] = useState(false);

  const setDatasourceUid = useCallback((uid: string, name?: string) => {
    sharedCoordinatesRef.current = null;
    restoringSharedClusterRef.current = false;
    setClusterReady(false);
    setError(null);
    setDatasourceUidState(uid);
    setDatasourceName(name || null);
    locationService.partial(shareCoordinateUpdate(uid, undefined), true);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ uid, name }));
    } catch { /* ignore */ }
    
    // Sync with connectionStore so pages see the connection as active
    const connectionStore = useConnectionStore.getState();
    connectionStore._setGrafanaDatasource(uid, name || 'ClickHouse');
  }, []);

  useEffect(() => {
    if (!datasourceUid) return;
    const resolvedName = datasourceName || 'ClickHouse';
    useConnectionStore.getState()._setGrafanaDatasource(datasourceUid, resolvedName);
  }, [datasourceUid, datasourceName]);

  // Auto-select datasource if none is stored and exactly one exists
  useEffect(() => {
    if (datasourceUid || sharedCoordinatesRef.current) return;
    const list = listClickHouseDatasources();
    if (list.length === 1) {
      setDatasourceUid(list[0].uid, list[0].name);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const services = useMemo(() => {
    if (!datasourceUid) {
      return null;
    }

    try {
      const queryFn: AdapterQueryFn = async (sql, refId) => {
        const ds = await getDataSourceSrv().get(datasourceUid);
        const response = await lastValueFrom(
          ds.query({
            targets: [{
              refId,
              rawSql: sql,
              format: 1,
              datasource: { uid: datasourceUid, type: 'grafana-clickhouse-datasource' },
            }],
            range: { from: dateTime(), to: dateTime(), raw: { from: 'now', to: 'now' } },
            // Unique per call: Grafana de-dupes queries sharing a requestId
            // by aborting the in-flight one, which would cancel concurrent
            // queries our services fire during a single render.
            requestId: `tracehouse-${refId}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
            interval: '1s',
            intervalMs: 1000,
            scopedVars: {},
            timezone: 'browser',
            app: 'tracehouse',
            startTime: Date.now(),
          } as Parameters<typeof ds.query>[0])
        );
        if (response.error) {
          const err = response.error as { message?: string };
          throw new Error(typeof response.error === 'string' ? response.error : err.message ?? 'Query failed');
        }
        return (response.data ?? []) as AdapterFrame[];
      };
      const rawAdapter = new GrafanaAdapter(queryFn);
      const adapter = new ClusterAwareAdapter(rawAdapter);
      const envDetector = new EnvironmentDetector(adapter);

      const svcs = {
        adapter,
        databaseExplorer: new DatabaseExplorer(adapter),
        mergeTracker: new MergeTracker(adapter),
        metricsCollector: new MetricsCollector(adapter),
        queryAnalyzer: new QueryAnalyzer(adapter, envDetector),
        interactiveQueryService: new InteractiveQueryService(adapter),
        queryExecutionAnalysisService: new QueryExecutionAnalysisService(adapter),
        columnCostService: new ColumnCostService(adapter),
        timelineService: new TimelineService(adapter),
        eventsService: new EventsService(adapter),
        eventContextService: new EventContextService(adapter),
        traceService: new TraceService(adapter),
        analyticsService: new AnalyticsService(adapter),
        observabilityMapService: new ObservabilityMapService(adapter),
        environmentDetector: envDetector,
      };
      return { svcs, clusterAdapter: adapter };
    } catch (e) {
      console.error('[ServiceProvider] Failed to create services:', e);
      setError(e instanceof Error ? e.message : String(e));
      return null;
    }
  }, [datasourceUid]);

  // Unwrap for context consumers
  const unresolvedServices = services?.svcs ?? null;
  const clickHouseServices = clusterReady ? unresolvedServices : null;

  useEffect(() => {
    if (clickHouseServices) {
      setError(null);
    }
  }, [clickHouseServices]);

  // Probe monitoring capabilities when services change (same as standalone frontend)
  useEffect(() => {
    const capStore = useMonitoringCapabilitiesStore.getState();
    if (!clickHouseServices) {
      capStore.reset();
      return;
    }

    capStore.setProbeStatus('probing');
    const svc = new MonitoringCapabilitiesService(clickHouseServices.adapter);
    let cancelled = false;

    svc.probe().then(caps => {
      if (!cancelled) {
        capStore.setCapabilities(caps);
      }
    }).catch(err => {
      if (!cancelled) {
        capStore.setProbeError(err instanceof Error ? err.message : 'Failed to probe monitoring capabilities');
      }
    });

    return () => { cancelled = true; };
  }, [clickHouseServices]);

  // Detect cluster topology when services change (same as standalone frontend)
  const { cluster: preferredCluster } = usePluginConfig();
  useEffect(() => {
    const clusterStore = useClusterStore.getState();
    if (!unresolvedServices) {
      clusterStore.reset();
      setClusterReady(false);
      return;
    }

    let cancelled = false;
    ClusterService.detect(unresolvedServices.adapter, preferredCluster).then(info => {
      if (!cancelled) {
        const sharedCoordinates = sharedCoordinatesRef.current;
        if (sharedCoordinates?.datasourceUid === datasourceUid) {
          if (sharedCoordinates.clusterName === undefined) {
            clusterStore.setCluster(info);
            services?.clusterAdapter.setClusterName(info.clusterName);
            setClusterReady(true);
            setError(null);
          } else if (sharedCoordinates.clusterName === null) {
            clusterStore.setCluster({ ...info, clusterName: null, replicaCount: 1, shardCount: 1 });
            services?.clusterAdapter.setClusterName(null);
            setClusterReady(true);
            setError(null);
          } else {
            const requested = info.availableClusters.find(c => c.name === sharedCoordinates.clusterName);
            if (!requested) {
              clusterStore.setCluster(info);
              setClusterReady(false);
              setError(`The shared cluster "${sharedCoordinates.clusterName}" is unavailable for this datasource.`);
            } else {
              clusterStore.setCluster({
                ...info,
                clusterName: requested.name,
                replicaCount: requested.replicaCount,
                shardCount: requested.shardCount,
              });
              services?.clusterAdapter.setClusterName(requested.name);
              setClusterReady(true);
              setError(null);
            }
          }
          restoringSharedClusterRef.current = false;
          return;
        }

        // Check if user has a saved override for this datasource
        const userOverride = datasourceUid ? loadClusterOverride(datasourceUid) : null;
        const hasOverride = userOverride && info.availableClusters.some(c => c.name === userOverride);
        if (hasOverride) {
          const match = info.availableClusters.find(c => c.name === userOverride)!;
          clusterStore.setCluster({
            ...info,
            clusterName: match.name,
            replicaCount: match.replicaCount,
            shardCount: match.shardCount,
          });
          services?.clusterAdapter.setClusterName(match.name);
        } else {
          clusterStore.setCluster(info);
          services?.clusterAdapter.setClusterName(info.clusterName);
        }
        setClusterReady(true);
        setError(null);
      }
    }).catch((err) => {
      console.error('[ClusterDetect] Cluster topology detection failed, falling back to single-node:', err);
      if (!cancelled) {
        clusterStore.setCluster({ clusterName: null, replicaCount: 1, shardCount: 1, availableClusters: [] });
        if (sharedCoordinatesRef.current?.clusterName) {
          setClusterReady(false);
          setError(`The shared cluster "${sharedCoordinatesRef.current.clusterName}" could not be resolved.`);
        } else {
          services?.clusterAdapter.setClusterName(null);
          setClusterReady(true);
        }
        restoringSharedClusterRef.current = false;
      }
    });

    return () => { cancelled = true; };
  }, [unresolvedServices, services, preferredCluster, datasourceUid]);

  // Sync ClusterAwareAdapter when user switches cluster via dropdown
  useEffect(() => {
    if (!services) return;
    return useClusterStore.subscribe((state, prev) => {
      if (state.clusterName !== prev.clusterName) {
        services.clusterAdapter.setClusterName(state.clusterName);
        if (restoringSharedClusterRef.current) return;
        locationService.partial(shareCoordinateUpdate(datasourceUid!, state.clusterName), true);
        setClusterReady(true);
        setError(null);
        // Persist user's choice per datasource
        if (datasourceUid && !restoringSharedClusterRef.current) {
          saveClusterOverride(datasourceUid, state.clusterName);
        }
      }
    });
  }, [services, datasourceUid]);

  const value: ServiceContextValue = {
    services: clickHouseServices,
    datasourceUid,
    datasourceName,
    setDatasourceUid,
    error,
    isLoading: Boolean(datasourceUid && !clusterReady && !error),
  };

  return (
    <ServiceContext.Provider value={value}>
      <ClickHouseContext.Provider value={clickHouseServices}>
        {children}
      </ClickHouseContext.Provider>
    </ServiceContext.Provider>
  );
}

export function useServices(): ServiceContextValue {
  const ctx = useContext(ServiceContext);
  if (!ctx) {
    throw new Error('useServices must be used within ServiceProvider');
  }
  return ctx;
}
