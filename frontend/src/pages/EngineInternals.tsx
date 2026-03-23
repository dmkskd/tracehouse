/**
 * EngineInternals - Deep dive into ClickHouse engine metrics
 * 
 * Shows:
 * - Memory X-Ray (jemalloc, caches, subsystems)
 * - CPU Core Map (per-core utilization)
 * - Thread Pools (utilization and saturation)
 * - Primary Key Index by Table
 * - Dictionaries in Memory
 * - Per-Query Internals (when a query is selected)
 */

import { useEffect, useRef, useCallback, useState, useMemo } from 'react';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useConnectionStore } from '../stores/connectionStore';
import { useClusterStore } from '../stores/clusterStore';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../stores/refreshSettingsStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useEngineInternalsStore, EngineInternalsPoller } from '../stores/engineInternalsStore';
import { EngineInternalsService, HostTargetedAdapter } from '@tracehouse/core';
import { TruncatedHost } from '../components/common/TruncatedHost';
import { DocsLink } from '../components/common/DocsLink';
import {
  MemoryXRay,
  CPUCoreMap,
  ThreadPoolsViz,
  PKIndexTable,
  DictionariesTable,
  MonitoringCapabilitiesCard,
  CPUSamplingCard,
  CoreTimelineCard,
} from '../components/engine-internals';

// No Connection Component
const NoConnection: React.FC<{ onConnect: () => void }> = ({ onConnect }) => (
  <div className="flex flex-col items-center justify-center py-16">
    <div 
      className="w-16 h-16 rounded-full flex items-center justify-center text-xl font-bold mb-4"
      style={{ background: 'var(--bg-tertiary)', color: 'var(--text-muted)' }}
    >
      --
    </div>
    <h3 className="text-lg font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>
      No Connection
    </h3>
    <p className="text-sm mb-4 text-center max-w-md" style={{ color: 'var(--text-secondary)' }}>
      Connect to a ClickHouse server to view engine internals
    </p>
    <button className="btn btn-primary" onClick={onConnect}>
      Add Connection
    </button>
  </div>
);

export const EngineInternals: React.FC = () => {
  const services = useClickHouseServices();
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const { clusterName } = useClusterStore();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const manualRefreshTick = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const {
    data,
    pollingStatus,
    lastError,
    clearData,
  } = useEngineInternalsStore();

  const pollerRef = useRef<EngineInternalsPoller | null>(null);

  // Cluster host selector state
  const [clusterHosts, setClusterHosts] = useState<string[]>([]);
  const [selectedHost, setSelectedHost] = useState<string | null>(null); // null = connected node

  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;

  const handleOpenConnectionForm = useCallback(() => {
    setConnectionFormOpen(true);
  }, [setConnectionFormOpen]);

  // Fetch cluster hosts on connect
  useEffect(() => {
    if (!services || !isConnected) { setClusterHosts([]); setSelectedHost(null); return; }
    (async () => {
      // Try metric_log first (has actual reporting hosts)
      let hosts = await services.metricsCollector.getClusterHosts();
      // Fallback: query system.clusters for configured hosts
      if (hosts.length <= 1 && clusterName) {
        try {
          const rows = await services.adapter.executeQuery<{ host: string }>(
            `SELECT DISTINCT host_name AS host FROM system.clusters WHERE cluster = '${clusterName}' ORDER BY host`
          );
          const fallbackHosts = rows.map(r => String(r.host)).filter(Boolean);
          if (fallbackHosts.length > 1) hosts = fallbackHosts;
        } catch { /* ignore */ }
      }
      setClusterHosts(hosts);
      if (hosts.length > 1) setSelectedHost(prev => prev && hosts.includes(prev) ? prev : hosts[0]);
    })();
  }, [services, isConnected, clusterName]);

  // Reset selected host if it disappears from the list
  useEffect(() => {
    if (selectedHost && clusterHosts.length > 1 && !clusterHosts.includes(selectedHost)) {
      setSelectedHost(clusterHosts[0]);
    }
  }, [clusterHosts, selectedHost]);

  // Effective adapter: host-targeted when a specific cluster host is selected
  const effectiveAdapter = useMemo(() => {
    if (!services) return undefined;
    if (selectedHost && clusterName) {
      return new HostTargetedAdapter(services.adapter, clusterName, selectedHost);
    }
    return services.adapter;
  }, [services, selectedHost, clusterName]);

  // Setup polling for engine internals data
  useEffect(() => {
    if (pollerRef.current) {
      pollerRef.current.stop();
      pollerRef.current = null;
    }

    if (!services || !isConnected) {
      clearData();
      return;
    }

    // Use the memoized effective adapter (host-targeted in cluster mode)
    const adapter = effectiveAdapter ?? services.adapter;

    const engineInternalsService = new EngineInternalsService(adapter);
    const intervalMs = refreshRateSeconds > 0 ? clampToAllowed(refreshRateSeconds, refreshConfig) * 1000 : 5000;
    pollerRef.current = new EngineInternalsPoller(engineInternalsService, intervalMs);
    if (refreshRateSeconds > 0) pollerRef.current.start();

    return () => {
      if (pollerRef.current) {
        pollerRef.current.stop();
        pollerRef.current = null;
      }
    };
  }, [services, isConnected, clearData, effectiveAdapter, refreshRateSeconds, refreshConfig, manualRefreshTick]);

  // No connection state
  if (!activeProfileId || !isConnected) {
    return (
      <div className="page-layout">
        <div>
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
            Engine Internals
          </h1>
        </div>
        <div className="card">
          <NoConnection onConnect={handleOpenConnectionForm} />
        </div>
      </div>
    );
  }

  return (
    <div className="page-layout">
      {/* Header */}
      <div>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
              Engine Internals
            </h1>
            <DocsLink path="/features/engine-internals" />
          </div>
        </div>
        {/* Host selector — only show if multiple hosts */}
        {clusterHosts.length > 1 && (
          <div className="tabs" style={{ width: 'fit-content', marginTop: '24px' }}>
            {clusterHosts.map(host => (
              <button
                key={host}
                className={`tab ${selectedHost === host ? 'active' : ''}`}
                onClick={() => setSelectedHost(host)}
              >
                <TruncatedHost name={host} />
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Error Banner */}
      {lastError && (
        <div className="card p-3" style={{ borderColor: 'var(--accent-red)', borderLeftWidth: '3px' }}>
          <span className="text-sm" style={{ color: 'var(--accent-red)' }}>{lastError}</span>
        </div>
      )}

      {/* Main Content */}
      {data ? (
        <>
          {/* CPU Core Map and CPU Sampling Attribution - Side by Side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {data.cpuCores && <CPUCoreMap cores={data.cpuCores} meta={data.cpuCoresMeta} />}
            <CPUSamplingCard key={selectedHost ?? '_'} adapter={effectiveAdapter} />
          </div>

          {/* Per-Core Timeline - Full Width */}
          <CoreTimelineCard key={selectedHost ?? '_'} cpuCoresMeta={data.cpuCoresMeta} adapter={effectiveAdapter} />

          {/* Thread Pools and Monitoring Capabilities - Side by Side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {data.threadPools && <ThreadPoolsViz pools={data.threadPools} />}
            <MonitoringCapabilitiesCard />
          </div>

          {/* Memory Breakdown and PK Index - Side by Side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {data.memoryXRay && <MemoryXRay memoryXRay={data.memoryXRay} />}
            {data.pkIndex && <PKIndexTable entries={data.pkIndex} />}
          </div>

          {/* Dictionaries - Full Width */}
          {data.dictionaries && <DictionariesTable dictionaries={data.dictionaries} />}
        </>
      ) : pollingStatus === 'polling' ? (
        <div className="card">
          <div className="card-body text-center py-10" style={{ color: 'var(--text-muted)' }}>
            Loading engine internals data...
          </div>
        </div>
      ) : null}
    </div>
  );
};

export default EngineInternals;
