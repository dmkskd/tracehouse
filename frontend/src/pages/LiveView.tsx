/**
 * Overview (Legacy LiveView) - Real-time resource attribution and active operations monitoring
 * 
 * Note: This page is kept for backward compatibility. The main Overview page is preferred.
 */

import { useEffect, useRef, useCallback } from 'react';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useConnectionStore } from '../stores/connectionStore';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../stores/refreshSettingsStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useOverviewStore, OverviewPoller } from '../stores/overviewStore';
import { OverviewService } from '@tracehouse/core';
import {
  ResourceAttributionBar,
  SummaryCards,
  RunningQueriesTable,
  ActiveMergesTable,
  ReplicationSummary,
  AlertBanner,
} from '../components/overview';

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
      Connect to a ClickHouse server to view live resource attribution
    </p>
    <button className="btn btn-primary" onClick={onConnect}>
      Add Connection
    </button>
  </div>
);

export const LiveView: React.FC = () => {
  const services = useClickHouseServices();
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const manualRefreshTick = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const {
    data,
    selectedResource,
    expandedQueryId,
    pollingStatus,
    lastError,
    setSelectedResource,
    toggleExpandedQuery,
    clearData,
  } = useOverviewStore();

  const pollerRef = useRef<OverviewPoller | null>(null);

  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;

  const handleOpenConnectionForm = useCallback(() => {
    setConnectionFormOpen(true);
  }, [setConnectionFormOpen]);

  // Setup polling for overview data
  useEffect(() => {
    if (pollerRef.current) {
      pollerRef.current.stop();
      pollerRef.current = null;
    }

    if (!services || !isConnected) {
      clearData();
      return;
    }

    // Create OverviewService from the adapter
    const overviewService = new OverviewService(services.adapter, {}, services.environmentDetector);
    const intervalMs = refreshRateSeconds > 0 ? clampToAllowed(refreshRateSeconds, refreshConfig) * 1000 : 5000;
    pollerRef.current = new OverviewPoller(overviewService, intervalMs);
    if (refreshRateSeconds > 0) pollerRef.current.start();

    return () => {
      if (pollerRef.current) {
        pollerRef.current.stop();
        pollerRef.current = null;
      }
    };
  }, [services, isConnected, clearData, refreshRateSeconds, refreshConfig, manualRefreshTick]);

  // No connection state
  if (!activeProfileId || !isConnected) {
    return (
      <div className="space-y-6" style={{ padding: '24px', background: 'var(--bg-primary)', minHeight: '100%' }}>
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
            Overview
          </h1>
        </div>
        <div className="card">
          <NoConnection onConnect={handleOpenConnectionForm} />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6" style={{ padding: '24px', background: 'var(--bg-primary)', minHeight: '100%' }}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
            Overview
          </h1>
          {data?.serverInfo && (
            <span className="text-sm" style={{ color: 'var(--text-muted)' }}>
              {data.serverInfo.hostname} • v{data.serverInfo.version} • up {data.serverInfo.uptime}
            </span>
          )}
        </div>
      </div>

      {/* Error Banner */}
      {lastError && (
        <div className="card p-3" style={{ borderColor: 'var(--accent-red)', borderLeftWidth: '3px' }}>
          <span className="text-sm" style={{ color: 'var(--accent-red)' }}>{lastError}</span>
        </div>
      )}

      {/* Alert Banner */}
      {data?.alerts && <AlertBanner alerts={data.alerts} />}

      {/* Resource Attribution Bar + Summary Cards */}
      <div className="space-y-6">
        {data?.resourceAttribution && (
          <div className="card">
            <div className="card-body">
              <ResourceAttributionBar
                attribution={data.resourceAttribution}
                selectedResource={selectedResource}
                onResourceChange={setSelectedResource}
              />
            </div>
          </div>
        )}

        {data && (
          <SummaryCards
            attribution={data.resourceAttribution}
            replication={data.replication}
          />
        )}
      </div>

      {/* Running Queries - Full Width */}
      {data && (
        <div className="pt-4">
          <RunningQueriesTable
            queries={data.runningQueries}
            expandedQueryId={expandedQueryId}
            onToggleExpand={toggleExpandedQuery}
            maxRows={5}
          />
        </div>
      )}

      {/* Active Merges + Mutations - Full Width, Stacked */}
      <div className="pt-4 space-y-6">
        {data && <ActiveMergesTable merges={data.activeMerges} maxRows={5} />}
      </div>

      {/* Replication Summary - if there's replication data */}
      {data?.replication && (data.replication.maxDelay > 0 || data.replication.queueSize > 0) && (
        <ReplicationSummary replication={data.replication} />
      )}

      {/* Loading State */}
      {!data && pollingStatus === 'polling' && (
        <div className="card">
          <div className="card-body text-center py-10" style={{ color: 'var(--text-muted)' }}>
            Loading overview data...
          </div>
        </div>
      )}
    </div>
  );
};

export default LiveView;
