/**
 * ClickHouseProvider — React context that provides an IClickHouseAdapter
 * and pre-built service instances to the component tree.
 *
 * The adapter is created from the active connection profile stored in
 * the connectionStore. When no profile is active the context value is null,
 * allowing consumers to show a connection form.
 *
 * connection profile and injects it into Service_Layer classes.
 */

import {
  useMemo,
  useEffect,
  useRef,
  type ReactNode,
} from 'react';
import {
  DatabaseExplorer,
  QueryAnalyzer,
  MetricsCollector,
  MergeTracker,
  TimelineService,
  TraceService,
  MonitoringCapabilitiesService,
  AnalyticsService,
  ClusterService,
  ClusterAwareAdapter,
  EnvironmentDetector,
} from '@tracehouse/core';
import { BrowserAdapter } from '@tracehouse/core/adapters/browser-adapter';
import { ProxyAdapter } from '@tracehouse/core';
import type { ConnectionConfig } from '@tracehouse/core';
import { useConnectionStore } from '../stores/connectionStore';
import { useProxyStore } from '../stores/proxyStore';
import { useMonitoringCapabilitiesStore } from '../stores/monitoringCapabilitiesStore';
import { useClusterStore } from '../stores/clusterStore';
import { useProfileEventDescriptionsStore } from '../stores/profileEventDescriptionsStore';
import { useEnvironmentStore } from '../stores/environmentStore';
import { 
  ClickHouseContext, 
  type ClickHouseServices,
  useClickHouseServices,
  useRequiredClickHouseServices,
} from '@tracehouse/ui-shared';

// Re-export hooks for backward compatibility
export { useClickHouseServices, useRequiredClickHouseServices };
export type { ClickHouseServices };

// ---------------------------------------------------------------------------
// Helper: build services from a ConnectionConfig
// ---------------------------------------------------------------------------

function buildServices(config: ConnectionConfig, proxyUrl?: string | null): {
  services: ClickHouseServices;
  clusterAdapter: ClusterAwareAdapter;
  close: () => Promise<void>;
} {
  const rawAdapter = proxyUrl
    ? new ProxyAdapter(config, proxyUrl)
    : new BrowserAdapter(config);
  const adapter = new ClusterAwareAdapter(rawAdapter);
  const envDetector = new EnvironmentDetector(adapter);
  return {
    services: {
      adapter,
      databaseExplorer: new DatabaseExplorer(adapter),
      queryAnalyzer: new QueryAnalyzer(adapter, envDetector),
      metricsCollector: new MetricsCollector(adapter),
      mergeTracker: new MergeTracker(adapter),
      timelineService: new TimelineService(adapter),
      traceService: new TraceService(adapter),
      analyticsService: new AnalyticsService(adapter),
      environmentDetector: envDetector,
    },
    clusterAdapter: adapter,
    close: () => 'close' in rawAdapter && typeof rawAdapter.close === 'function'
      ? (rawAdapter as BrowserAdapter).close()
      : Promise.resolve(),
  };
}

// ---------------------------------------------------------------------------
// Provider component
// ---------------------------------------------------------------------------

export interface ClickHouseProviderProps {
  children: ReactNode;
}

export function ClickHouseProvider({ children }: ClickHouseProviderProps) {
  const profiles = useConnectionStore((s) => s.profiles);
  const activeProfileId = useConnectionStore((s) => s.activeProfileId);
  const proxyUrl = useProxyStore((s) => s.enabled ? s.url : null);

  // Resolve the active profile's config (if any)
  const activeConfig = useMemo<ConnectionConfig | null>(() => {
    if (!activeProfileId) return null;
    const profile = profiles.find((p) => p.id === activeProfileId);
    if (!profile) return null;
    // The stored profile has password stripped — build a config with empty
    // password. The user will need to supply the password at connect-time
    // (handled by the connection form / store).
    return {
      host: profile.config.host,
      port: profile.config.port,
      user: profile.config.user ?? 'default',
      password: ((profile.config as unknown as Record<string, unknown>).password as string) ?? '',
      database: profile.config.database ?? 'default',
      secure: profile.config.secure ?? false,
      connect_timeout: profile.config.connect_timeout ?? 10,
      send_receive_timeout: profile.config.send_receive_timeout ?? 300,
    };
  }, [profiles, activeProfileId]);

  // Stable key to detect config changes — include activeProfileId so that
  // switching between profiles with identical host/port/user still triggers
  // a full service rebuild (different passwords, different logical connection).
  const configKey = activeConfig
    ? `${activeProfileId}:${activeConfig.host}:${activeConfig.port}:${activeConfig.user}:${activeConfig.password}:${activeConfig.database}:${activeConfig.secure}:${proxyUrl ?? 'direct'}`
    : null;

  // Track the previous close function so we can clean up
  const closeRef = useRef<(() => Promise<void>) | null>(null);
  const clusterAdapterRef = useRef<ClusterAwareAdapter | null>(null);

  const value = useMemo<ClickHouseServices | null>(() => {
    // Clean up previous adapter
    if (closeRef.current) {
      closeRef.current().catch(() => {/* ignore close errors */});
      closeRef.current = null;
    }
    clusterAdapterRef.current = null;

    if (!activeConfig) return null;

    const { services, clusterAdapter, close } = buildServices(activeConfig, proxyUrl);
    closeRef.current = close;
    clusterAdapterRef.current = clusterAdapter;
    return services;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [configKey]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (closeRef.current) {
        closeRef.current().catch(() => {/* ignore close errors */});
        closeRef.current = null;
      }
    };
  }, []);

  // Probe monitoring capabilities when connection changes
  useEffect(() => {
    const capStore = useMonitoringCapabilitiesStore.getState();
    if (!value) {
      capStore.reset();
      return;
    }

    capStore.setProbeStatus('probing');
    const svc = new MonitoringCapabilitiesService(value.adapter);
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
  }, [value]);

  // Detect cluster topology when connection changes
  useEffect(() => {
    const clusterStore = useClusterStore.getState();
    if (!value) {
      clusterStore.reset();
      return;
    }

    let cancelled = false;
    ClusterService.detect(value.adapter).then(info => {
      if (!cancelled) {
        clusterStore.setCluster(info);
        // Set cluster name on the wrapper so all queries get rewritten automatically
        clusterAdapterRef.current?.setClusterName(info.clusterName);
        if (info.clusterName) {
          console.log(`[ClusterDetect] Cluster '${info.clusterName}' detected (${info.replicaCount} replicas) — queries will use clusterAllReplicas()`);
        } else {
          console.log('[ClusterDetect] No cluster detected — single-node mode');
        }
      }
    }).catch(() => {
      if (!cancelled) {
        // Mark detection as complete even on failure — otherwise the UI
        // stays stuck on "Detecting cluster topology..." forever.
        clusterStore.setCluster({ clusterName: null, replicaCount: 1, shardCount: 1 });
      }
    });

    return () => { cancelled = true; };
  }, [value]);

  // Detect runtime environment (container, k8s, cgroup limits) when connection changes
  useEffect(() => {
    const envStore = useEnvironmentStore.getState();
    if (!value) {
      envStore.reset();
      return;
    }

    let cancelled = false;
    envStore.setProbing(true);
    value.environmentDetector.detect().then(info => {
      if (!cancelled) {
        envStore.setEnvironment(info);
        if (info.isCgroupLimited) {
          console.log(`[EnvDetect] Container detected: ${info.effectiveCores} vCPUs (host: ${info.hostCores}), k8s=${info.isKubernetes}`);
        }
      }
    }).catch(() => {
      if (!cancelled) envStore.reset();
    });

    return () => { cancelled = true; };
  }, [value]);

  // Fetch profile event descriptions at connection time, refresh every 60s
  useEffect(() => {
    const descStore = useProfileEventDescriptionsStore.getState();
    if (!value) {
      descStore.reset();
      return;
    }

    let cancelled = false;
    const fetch = () => {
      value.queryAnalyzer.fetchProfileEventDescriptions().then(map => {
        if (!cancelled) {
          descStore.setDescriptions(map);
        }
      }).catch(() => {});
    };

    fetch();
    const interval = setInterval(fetch, 60_000);

    return () => { cancelled = true; clearInterval(interval); };
  }, [value]);

  return (
    <ClickHouseContext.Provider value={value}>
      {children}
    </ClickHouseContext.Provider>
  );
}
