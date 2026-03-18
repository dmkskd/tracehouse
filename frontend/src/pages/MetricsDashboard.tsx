/**
 * MetricsDashboard - Server metrics with dark theme
 */

import React, { useEffect, useRef, useCallback } from 'react';
import { useConnectionStore } from '../stores/connectionStore';
import { 
  useMetricsStore, 
  MetricsWebSocket,
  calculateMemoryPercentage,
} from '../stores/metricsStore';
import { useTimeSeriesStore } from '../stores/timeSeriesStore';
import { 
  TimeSeriesChart, 
  MetricSelector, 
} from '../components/metrics/TimeSeriesChart';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { formatDuration } from '../utils/formatters';

// Format bytes to human readable
const formatBytes = (bytes: number): string => {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
};


// Stat Card Component
const StatCard: React.FC<{
  label: string;
  value: string | number;
  subtitle?: string;
  icon?: string;
  trend?: 'up' | 'down' | 'neutral';
  color?: 'default' | 'green' | 'yellow' | 'red' | 'purple' | 'blue';
}> = ({ label, value, subtitle, color = 'default' }) => {
  const colorClasses = {
    default: '',
    green: 'border-l-2 border-l-[var(--accent-green)]',
    yellow: 'border-l-2 border-l-[var(--accent-yellow)]',
    red: 'border-l-2 border-l-[var(--accent-red)]',
    purple: 'border-l-2 border-l-[var(--accent-primary)]',
    blue: 'border-l-2 border-l-[var(--accent-blue)]',
  };

  return (
    <div className={`stat-card ${colorClasses[color]}`}>
      <div className="flex items-start justify-between">
        <div>
          <div className="stat-value">{value}</div>
          <div className="stat-label">{label}</div>
          {subtitle && (
            <div className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
              {subtitle}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

// Highlight Item Component (like in the reference)
const HighlightItem: React.FC<{
  icon: string;
  iconColor: 'green' | 'purple' | 'blue' | 'yellow' | 'red';
  title: string;
  badge?: string;
}> = ({ iconColor, title, badge }) => {
  const iconBg = {
    green: 'rgba(63, 185, 80, 0.15)',
    purple: 'rgba(139, 92, 246, 0.15)',
    blue: 'rgba(88, 166, 255, 0.15)',
    yellow: 'rgba(210, 153, 34, 0.15)',
    red: 'rgba(248, 81, 73, 0.15)',
  };
  
  const iconTextColor = {
    green: 'var(--accent-green)',
    purple: 'var(--accent-primary)',
    blue: 'var(--accent-blue)',
    yellow: 'var(--accent-yellow)',
    red: 'var(--accent-red)',
  };

  return (
    <div className="highlight-card">
      <div 
        className="w-8 h-8 rounded-md flex items-center justify-center text-xs font-bold"
        style={{ background: iconBg[iconColor], color: iconTextColor[iconColor] }}
      >
        !
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-sm truncate" style={{ color: 'var(--text-primary)' }}>
          {title}
        </div>
      </div>
      {badge && (
        <span className={`badge badge-${iconColor}`}>{badge}</span>
      )}
    </div>
  );
};

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
      Connect to a ClickHouse server to view real-time metrics
    </p>
    <button className="btn btn-primary" onClick={onConnect}>
      Add Connection
    </button>
  </div>
);

export const MetricsDashboard: React.FC = () => {
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const { 
    metrics, wsStatus, wsError, clearMetrics, thresholds, warnings 
  } = useMetricsStore();
  const {
    viewMode, selectedMetrics, toggleViewMode, toggleMetric,
    addDataPoint, getChartData, getStats, clearData, getDataPointCount,
  } = useTimeSeriesStore();
  const services = useClickHouseServices();

  const wsRef = useRef<MetricsWebSocket | null>(null);
  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;

  const handleOpenConnectionForm = useCallback(() => {
    setConnectionFormOpen(true);
  }, [setConnectionFormOpen]);

  useEffect(() => {
    if (metrics) addDataPoint(metrics);
  }, [metrics, addDataPoint]);

  useEffect(() => {
    if (wsRef.current) {
      wsRef.current.disconnect();
      wsRef.current = null;
    }
    if (!services || !isConnected) {
      clearMetrics();
      clearData();
      return;
    }
    wsRef.current = new MetricsWebSocket(services.metricsCollector);
    wsRef.current.connect();
    return () => {
      if (wsRef.current) {
        wsRef.current.disconnect();
        wsRef.current = null;
      }
    };
  }, [services, isConnected, clearMetrics, clearData]);

  if (!activeProfileId || !isConnected) {
    return (
      <div>
        <div className="flex items-center justify-between mb-6">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
            Server Metrics
          </h1>
        </div>
        <div className="card">
          <NoConnection onConnect={handleOpenConnectionForm} />
        </div>
      </div>
    );
  }

  const memoryPercentage = metrics 
    ? calculateMemoryPercentage(metrics.memory_used, metrics.memory_total)
    : 0;

  const chartData = getChartData();
  const dataPointCount = getDataPointCount();
  const trendStats = {
    cpu_usage: getStats('cpu_usage'),
    memory_percentage: getStats('memory_percentage'),
    disk_read_rate: getStats('disk_read_rate'),
    disk_write_rate: getStats('disk_write_rate'),
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
            Server Metrics
          </h1>
          {activeProfile && (
            <p className="text-sm mt-1" style={{ color: 'var(--text-secondary)' }}>
              {activeProfile.name} • {activeProfile.config.host}:{activeProfile.config.port}
            </p>
          )}
        </div>
        <div className="flex items-center gap-4">
          {/* View Toggle */}
          <div className="tabs">
            <button 
              className={`tab ${viewMode === 'snapshot' ? 'active' : ''}`}
              onClick={() => viewMode !== 'snapshot' && toggleViewMode()}
            >
              Snapshot
            </button>
            <button 
              className={`tab ${viewMode === 'trend' ? 'active' : ''}`}
              onClick={() => viewMode !== 'trend' && toggleViewMode()}
            >
              Trends
            </button>
          </div>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="CPU Usage"
          value={metrics ? `${metrics.cpu_usage.toFixed(1)}%` : '—'}
          color={warnings.cpu ? 'red' : 'blue'}
          subtitle={warnings.cpu ? `Above ${thresholds.cpu_warning}% threshold` : undefined}
        />
        <StatCard
          label="Memory"
          value={metrics ? `${memoryPercentage.toFixed(1)}%` : '—'}
          color={warnings.memory ? 'red' : 'purple'}
          subtitle={metrics ? `${formatBytes(metrics.memory_used)} / ${formatBytes(metrics.memory_total)}` : undefined}
        />
        <StatCard
          label="Disk Read"
          value={metrics ? formatBytes(metrics.disk_read_bytes) : '—'}
          color="green"
        />
        <StatCard
          label="Disk Write"
          value={metrics ? formatBytes(metrics.disk_write_bytes) : '—'}
          color="yellow"
        />
      </div>

      {/* Highlights Section */}
      {(warnings.cpu || warnings.memory) && (
        <div>
          <div className="section-header">Alerts</div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {warnings.cpu && (
              <HighlightItem
                icon="!"
                iconColor="red"
                title={`CPU usage at ${metrics?.cpu_usage.toFixed(1)}%`}
                badge="Warning"
              />
            )}
            {warnings.memory && (
              <HighlightItem
                icon="!"
                iconColor="red"
                title={`Memory usage at ${memoryPercentage.toFixed(1)}%`}
                badge="Warning"
              />
            )}
          </div>
        </div>
      )}

      {/* Trend Chart */}
      {viewMode === 'trend' && (
        <div className="card">
          <div className="card-header flex items-center justify-between">
            <div>
              <span>Metrics Trends</span>
              <span className="ml-2 text-xs" style={{ color: 'var(--text-muted)' }}>
                {dataPointCount} data points
              </span>
            </div>
            <MetricSelector
              selectedMetrics={selectedMetrics}
              onToggleMetric={toggleMetric}
            />
          </div>
          <div className="card-body">
            <TimeSeriesChart
              data={chartData}
              selectedMetrics={selectedMetrics}
              height={350}
              stats={trendStats}
            />
          </div>
        </div>
      )}

      {/* Server Info */}
      {metrics?.uptime_seconds !== undefined && viewMode === 'snapshot' && (
        <div>
          <div className="section-header">Server Info</div>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <StatCard
              label="Uptime"
              value={formatDuration(metrics.uptime_seconds)}
            />
          </div>
        </div>
      )}

      {/* Error Display */}
      {wsError && wsStatus === 'error' && (
        <div 
          className="card p-4 flex items-start gap-3"
          style={{ borderColor: 'var(--accent-red)' }}
        >
          <span className="text-xl font-bold" style={{ color: 'var(--accent-red)' }}>!</span>
          <div>
            <div className="font-medium" style={{ color: 'var(--accent-red)' }}>
              Connection Error
            </div>
            <div className="text-sm mt-1" style={{ color: 'var(--text-secondary)' }}>
              {wsError}
            </div>
            <button
              onClick={() => wsRef.current?.connect()}
              className="text-sm mt-2 underline"
              style={{ color: 'var(--accent-blue)' }}
            >
              Retry Connection
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default MetricsDashboard;
