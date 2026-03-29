import React, { createContext, useContext, useState, useEffect, useMemo, useCallback } from 'react';
import { getBackendSrv, getDataSourceSrv } from '@grafana/runtime';
import { GrafanaAdapter } from '@tracehouse/core/adapters/grafana-adapter';
import { ClusterAwareAdapter } from '@tracehouse/core/adapters/cluster-adapter';
import { ClusterService } from '@tracehouse/core/services/cluster-service';
import { DatabaseExplorer } from '@tracehouse/core/services/database-explorer';
import { MergeTracker } from '@tracehouse/core/services/merge-tracker';
import { MetricsCollector } from '@tracehouse/core/services/metrics-collector';
import { QueryAnalyzer } from '@tracehouse/core/services/query-analyzer';
import { TimelineService } from '@tracehouse/core/services/timeline-service';
import { TraceService } from '@tracehouse/core/services/trace-service';
import { AnalyticsService } from '@tracehouse/core/services/analytics-service';
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

export function ServiceProvider({ children }: { children: React.ReactNode }) {
  const [datasourceUid, setDatasourceUidState] = useState<string | null>(() => {
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
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const parsed = JSON.parse(stored);
        return parsed.name || null;
      }
    } catch { /* ignore */ }
    return null;
  });
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const setDatasourceUid = useCallback((uid: string, name?: string) => {
    setDatasourceUidState(uid);
    setDatasourceName(name || null);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ uid, name }));
    } catch { /* ignore */ }
    
    // Sync with connectionStore so pages see the connection as active
    const connectionStore = useConnectionStore.getState();
    connectionStore._setGrafanaDatasource(uid, name || 'ClickHouse');
  }, []);

  // Auto-select datasource if none is stored and exactly one exists
  useEffect(() => {
    if (datasourceUid) return;
    const list = getDataSourceSrv().getList({ type: 'grafana-clickhouse-datasource' });
    if (list.length === 1) {
      setDatasourceUid(list[0].uid, list[0].name);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const services = useMemo(() => {
    if (!datasourceUid) {
      console.log('[ServiceProvider] No datasource UID set');
      return null;
    }

    try {
      console.log('[ServiceProvider] Creating services for datasource:', datasourceUid);
      const rawAdapter = new GrafanaAdapter(
        { uid: datasourceUid, type: 'grafana-clickhouse-datasource' },
        () => getBackendSrv()
      );
      const adapter = new ClusterAwareAdapter(rawAdapter);
      const envDetector = new EnvironmentDetector(adapter);

      const svcs = {
        adapter,
        databaseExplorer: new DatabaseExplorer(adapter),
        mergeTracker: new MergeTracker(adapter),
        metricsCollector: new MetricsCollector(adapter),
        queryAnalyzer: new QueryAnalyzer(adapter, envDetector),
        timelineService: new TimelineService(adapter),
        traceService: new TraceService(adapter),
        analyticsService: new AnalyticsService(adapter),
        environmentDetector: envDetector,
      };
      console.log('[ServiceProvider] Services created successfully');
      return { svcs, clusterAdapter: adapter };
    } catch (e) {
      console.error('[ServiceProvider] Error creating services:', e);
      setError(e instanceof Error ? e.message : String(e));
      return null;
    }
  }, [datasourceUid]);

  // Unwrap for context consumers
  const clickHouseServices = services?.svcs ?? null;

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
  useEffect(() => {
    const clusterStore = useClusterStore.getState();
    if (!clickHouseServices) {
      clusterStore.reset();
      return;
    }

    let cancelled = false;
    ClusterService.detect(clickHouseServices.adapter).then(info => {
      if (!cancelled) {
        clusterStore.setCluster(info);
        // Set cluster name on the wrapper so all queries get rewritten
        services?.clusterAdapter.setClusterName(info.clusterName);
        if (info.clusterName) {
          console.log(`[ClusterDetect] Cluster '${info.clusterName}' detected (${info.replicaCount} replicas)`);
        } else {
          console.log('[ClusterDetect] No cluster detected — single-node mode');
        }
      }
    }).catch(() => {
      if (!cancelled) {
        clusterStore.setCluster({ clusterName: null, replicaCount: 1, shardCount: 1, availableClusters: [] });
      }
    });

    return () => { cancelled = true; };
  }, [clickHouseServices, services]);

  const value: ServiceContextValue = {
    services: clickHouseServices,
    datasourceUid,
    datasourceName,
    setDatasourceUid,
    error,
    isLoading,
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
