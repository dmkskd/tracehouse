/**
 * SystemOverview - Unified system dashboard
 * Top: Server metrics (CPU, Memory, Disk) with Snapshot/Trends toggle
 * Bottom: Background pools and active merges
 */

import React, { useEffect, useState, useCallback, useRef, useMemo } from 'react';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useConnectionStore } from '../stores/connectionStore';
import { 
  useMetricsStore, 
  MetricsWebSocket,
  calculateMemoryPercentage,
} from '../stores/metricsStore';
import { useTimeSeriesStore } from '../stores/timeSeriesStore';
import { TimeSeriesChart } from '../components/metrics/TimeSeriesChart';
import type { MergeInfo, BackgroundPoolMetrics, HistoricalMetricsPoint } from '@tracehouse/core';
import type { ChartDataPoint, TrendMetricType, MetricStats } from '../stores/timeSeriesStore';
import { formatBytes, formatDuration } from '../utils/formatters';


// Stat Card - matches MetricsDashboard style
const StatCard: React.FC<{
  label: string;
  value: string | number;
  subtitle?: string;
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
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
      {subtitle && (
        <div className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
          {subtitle}
        </div>
      )}
    </div>
  );
};

// Semi-circular gauge for pool utilization
const SemiGauge: React.FC<{
  value: number;
  max: number;
  label: string;
  color: string;
}> = ({ value, max, label, color }) => {
  const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0;
  const angle = (pct / 100) * 180;
  
  return (
    <div className="flex flex-col items-center">
      <div className="relative w-28 h-14 overflow-hidden">
        <div 
          className="absolute inset-0 rounded-t-full border-[6px]"
          style={{ borderBottomWidth: 0, borderColor: 'var(--border-secondary)' }}
        />
        <div 
          className="absolute inset-0 rounded-t-full border-[6px] origin-bottom"
          style={{ 
            borderBottomWidth: 0,
            borderColor: color,
            clipPath: `polygon(0 100%, 0 0, ${50 + 50 * Math.sin(angle * Math.PI / 180)}% ${100 - 100 * Math.cos(angle * Math.PI / 180)}%, 50% 100%)`,
            filter: `drop-shadow(0 0 6px ${color}50)`
          }}
        />
        <div className="absolute bottom-0 left-1/2 -translate-x-1/2 text-center">
          <div className="text-base font-semibold" style={{ color: 'var(--text-primary)' }}>{value}/{max}</div>
        </div>
      </div>
      <div className="text-sm mt-2 uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>{label}</div>
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

export const SystemOverview: React.FC = () => {
  const services = useClickHouseServices();
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const { metrics, clearMetrics, thresholds, warnings } = useMetricsStore();
  const { viewMode, selectedMetrics, toggleViewMode, toggleMetric } = useTimeSeriesStore();
  
  const [poolMetrics, setPoolMetrics] = useState<BackgroundPoolMetrics | null>(null);
  const [activeMerges, setActiveMerges] = useState<MergeInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  
  // Historical metrics state
  const [historicalData, setHistoricalData] = useState<HistoricalMetricsPoint[]>([]);
  const [timeRangeMinutes, setTimeRangeMinutes] = useState(15);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  
  const wsRef = useRef<MetricsWebSocket | null>(null);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
  
  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;
  
  const handleOpenConnectionForm = useCallback(() => {
    setConnectionFormOpen(true);
  }, [setConnectionFormOpen]);
  
  // Fetch background pool and merge data
  const fetchPoolData = useCallback(async () => {
    if (!services) return;
    try {
      const pools = await services.mergeTracker.getBackgroundPoolMetrics();
      setPoolMetrics(pools);
      const merges = await services.mergeTracker.getActiveMerges();
      setActiveMerges(merges);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch metrics');
    }
  }, [services]);
  
  // Fetch historical metrics from ClickHouse
  const fetchHistoricalMetrics = useCallback(async () => {
    if (!services) return;
    setIsLoadingHistory(true);
    setHistoryError(null);
    try {
      const toTime = new Date();
      const fromTime = new Date(toTime.getTime() - timeRangeMinutes * 60 * 1000);
      // Adjust interval based on time range for reasonable data density
      const intervalSeconds = Math.max(5, Math.floor(timeRangeMinutes * 60 / 200));
      
      console.log('[HistoricalMetrics] Fetching:', { fromTime, toTime, intervalSeconds });
      const data = await services.metricsCollector.getHistoricalMetrics(fromTime, toTime, intervalSeconds);
      console.log('[HistoricalMetrics] Got data:', data.length, 'points');
      if (data.length > 0) {
        const cpuValues = data.map(d => d.cpu_usage).filter(v => v > 0);
        console.log('[HistoricalMetrics] CPU stats:', {
          min: Math.min(...cpuValues),
          max: Math.max(...cpuValues),
          avg: cpuValues.reduce((a, b) => a + b, 0) / cpuValues.length,
          sample: data.slice(0, 3),
        });
      }
      setHistoricalData(data);
    } catch (err) {
      console.error('[HistoricalMetrics] Error:', err);
      let msg = 'Unknown error';
      if (err instanceof Error) {
        msg = err.message;
        // Include cause if available
        if ('cause' in err && err.cause instanceof Error) {
          msg += `: ${err.cause.message}`;
        }
      }
      setHistoryError(msg);
    } finally {
      setIsLoadingHistory(false);
    }
  }, [services, timeRangeMinutes]);
  
  // Setup WebSocket for real-time metrics (snapshot mode)
  useEffect(() => {
    if (wsRef.current) {
      wsRef.current.disconnect();
      wsRef.current = null;
    }
    if (!services || !isConnected) {
      clearMetrics();
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
  }, [services, isConnected, clearMetrics]);
  
  // Poll for pool/merge data
  useEffect(() => {
    if (services && isConnected) {
      fetchPoolData();
      pollingRef.current = setInterval(fetchPoolData, 2000);
    }
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
  }, [services, isConnected, fetchPoolData]);
  
  // Fetch historical data when switching to trends or changing time range
  useEffect(() => {
    if (viewMode === 'trend' && services && isConnected) {
      fetchHistoricalMetrics();
    }
  }, [viewMode, timeRangeMinutes, services, isConnected, fetchHistoricalMetrics]);
  
  // Convert historical data to chart format
  const chartData: ChartDataPoint[] = useMemo(() => {
    return historicalData.map(point => ({
      timestamp: point.timestamp,
      time: new Date(point.timestamp).toLocaleTimeString([], { 
        hour: '2-digit', 
        minute: '2-digit', 
        second: '2-digit' 
      }),
      cpu_usage: point.cpu_usage,
      memory_percentage: point.memory_total > 0 
        ? (point.memory_used / point.memory_total) * 100 
        : 0,
      memory_used: point.memory_used,
      memory_total: point.memory_total,
      disk_read_rate: point.disk_read_rate,
      disk_write_rate: point.disk_write_rate,
      network_send_rate: point.network_send_rate ?? 0,
      network_recv_rate: point.network_recv_rate ?? 0,
    }));
  }, [historicalData]);
  
  // Calculate stats from historical data
  const trendStats: Partial<Record<TrendMetricType, MetricStats | null>> = useMemo(() => {
    if (chartData.length === 0) return {};
    
    const calcStats = (values: number[]): MetricStats => ({
      min: Math.min(...values),
      max: Math.max(...values),
      avg: values.reduce((a, b) => a + b, 0) / values.length,
      current: values[values.length - 1],
    });
    
    return {
      cpu_usage: calcStats(chartData.map(d => d.cpu_usage)),
      memory_percentage: calcStats(chartData.map(d => d.memory_percentage)),
      disk_read_rate: calcStats(chartData.map(d => d.disk_read_rate)),
      disk_write_rate: calcStats(chartData.map(d => d.disk_write_rate)),
      network_send_rate: calcStats(chartData.map(d => d.network_send_rate)),
      network_recv_rate: calcStats(chartData.map(d => d.network_recv_rate)),
    };
  }, [chartData]);
  
  // No connection state
  if (!activeProfileId || !isConnected) {
    return (
      <div className="space-y-6" style={{ padding: '24px', background: 'var(--bg-primary)', minHeight: '100%' }}>
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
            System Overview
          </h1>
        </div>
        <div className="card">
          <NoConnection onConnect={handleOpenConnectionForm} />
        </div>
      </div>
    );
  }
  
  const memoryPercentage = metrics ? calculateMemoryPercentage(metrics.memory_used, metrics.memory_total) : 0;

  return (
    <div className="space-y-6" style={{ padding: '24px', background: 'var(--bg-primary)', minHeight: '100%' }}>
      {/* Header */}
      <div className="flex items-center justify-between" style={{ marginBottom: '24px' }}>
        <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
          System Overview
        </h1>
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

      {error && (
        <div className="card p-3 border-red-500/30" style={{ borderColor: 'var(--accent-red)' }}>
          <span className="text-sm" style={{ color: 'var(--accent-red)' }}>{error}</span>
        </div>
      )}

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

      {/* Trend Chart */}
      {viewMode === 'trend' && (
        <div className="card">
          <div className="card-header flex items-center justify-between">
            <div className="flex items-center gap-3">
              <span>Metrics Trends</span>
              {isLoadingHistory && (
                <span className="text-xs" style={{ color: 'var(--text-muted)' }}>Loading...</span>
              )}
              {!isLoadingHistory && chartData.length > 0 && (
                <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
                  {chartData.length} points
                </span>
              )}
              <button
                onClick={fetchHistoricalMetrics}
                disabled={isLoadingHistory}
                style={{
                  padding: '2px 8px',
                  fontSize: '11px',
                  borderRadius: '4px',
                  border: 'none',
                  cursor: isLoadingHistory ? 'not-allowed' : 'pointer',
                  background: 'var(--bg-tertiary)',
                  color: 'var(--text-secondary)',
                }}
              >
                ↻
              </button>
            </div>
            {/* Time Range Selector */}
            <div className="flex items-center gap-2">
              {[
                { label: '5m', minutes: 5 },
                { label: '15m', minutes: 15 },
                { label: '1h', minutes: 60 },
                { label: '6h', minutes: 360 },
                { label: '24h', minutes: 1440 },
              ].map(({ label, minutes }) => {
                const isActive = timeRangeMinutes === minutes;
                return (
                  <button
                    key={label}
                    onClick={() => setTimeRangeMinutes(minutes)}
                    style={{
                      padding: '4px 10px',
                      fontSize: '12px',
                      borderRadius: '4px',
                      border: 'none',
                      cursor: 'pointer',
                      background: isActive ? 'var(--accent-primary)' : 'var(--bg-tertiary)',
                      color: isActive ? 'white' : 'var(--text-secondary)',
                    }}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </div>
          <div className="card-body">
            {historyError ? (
              <div className="text-center py-10" style={{ color: 'var(--text-muted)' }}>
                {historyError}
              </div>
            ) : chartData.length === 0 && !isLoadingHistory ? (
              <div className="text-center py-10" style={{ color: 'var(--text-muted)' }}>
                No historical data available for this time range
              </div>
            ) : (
              <TimeSeriesChart 
                data={chartData} 
                selectedMetrics={selectedMetrics} 
                height={300} 
                stats={trendStats}
                onToggleMetric={toggleMetric}
              />
            )}
          </div>
        </div>
      )}

      {/* Server Info */}
      {metrics?.uptime_seconds !== undefined && viewMode === 'snapshot' && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard label="Uptime" value={formatDuration(metrics.uptime_seconds)} />
        </div>
      )}

      {/* Background Operations Section - only show in snapshot mode */}
      {viewMode === 'snapshot' && (
      <div className="pt-8" style={{ marginTop: '40px', borderTop: '1px solid var(--border-primary)' }}>
        <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)', marginBottom: '24px' }}>
          Background Operations
        </h1>
        
        {/* Thread Pools */}
        <div className="card mb-4">
          <div className="card-header">Thread Pools</div>
          <div className="card-body">
            <div className="flex justify-around items-center py-2">
              <SemiGauge 
                value={poolMetrics?.merge_pool_active || 0}
                max={poolMetrics?.merge_pool_size || 16}
                label="Merge"
                color="#f97316"
              />
              <SemiGauge 
                value={poolMetrics?.schedule_pool_active || 0}
                max={poolMetrics?.schedule_pool_size || 128}
                label="Schedule"
                color="#8b5cf6"
              />
              <SemiGauge 
                value={poolMetrics?.common_pool_active || 0}
                max={poolMetrics?.common_pool_size || 16}
                label="Common"
                color="#3b82f6"
              />
              <SemiGauge 
                value={poolMetrics?.move_pool_active || 0}
                max={poolMetrics?.move_pool_size || 8}
                label="Move"
                color="#10b981"
              />
              <div className="pl-10 flex gap-10" style={{ borderLeft: '1px solid var(--border-primary)' }}>
                <div className="text-center">
                  <div className="text-3xl font-semibold" style={{ color: 'var(--text-primary)' }}>
                    {(poolMetrics?.active_parts || 0).toLocaleString()}
                  </div>
                  <div className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>Active Parts</div>
                </div>
                <div className="text-center">
                  <div className="text-3xl font-semibold" style={{ color: 'var(--accent-red)' }}>
                    {poolMetrics?.outdated_parts || 0}
                  </div>
                  <div className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>Outdated Parts</div>
                  {poolMetrics?.outdated_parts_bytes ? (
                    <div className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
                      {formatBytes(poolMetrics.outdated_parts_bytes)}
                    </div>
                  ) : null}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Active Merges */}
        <div className="card">
          <div className="card-header flex items-center justify-between">
            <span>Active Merges</span>
            {activeMerges.length > 0 && (
              <span className="badge badge-yellow">{activeMerges.length}</span>
            )}
          </div>
          <div className="card-body">
            {activeMerges.length > 0 ? (
              <div className="space-y-3">
                {activeMerges.map((merge, i) => {
                  const pct = Math.round(merge.progress * 100);
                  return (
                    <div
                      key={`${merge.database}.${merge.table}:${merge.result_part_name}:${i}`}
                      className="p-4 rounded-lg"
                      style={{ background: 'var(--bg-tertiary)' }}
                    >
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-3 min-w-0 flex-1">
                          <span 
                            className="text-xs uppercase px-2 py-1 rounded font-medium"
                            style={{ background: 'rgba(249, 115, 22, 0.2)', color: '#f97316' }}
                          >
                            {merge.merge_type}
                          </span>
                          <span 
                            className="text-sm font-mono truncate"
                            style={{ color: 'var(--text-primary)' }}
                          >
                            {merge.database}.{merge.table}
                          </span>
                        </div>
                        <div className="flex items-center gap-6 text-sm" style={{ color: 'var(--text-muted)' }}>
                          <span>{merge.num_parts} parts</span>
                          <span>{formatBytes(merge.memory_usage)} mem</span>
                          <span>{formatBytes(merge.total_size_bytes_compressed)}</span>
                          <span>{merge.elapsed.toFixed(1)}s</span>
                          <span 
                            className="text-lg font-semibold w-12 text-right"
                            style={{ color: '#f97316' }}
                          >
                            {pct}%
                          </span>
                        </div>
                      </div>
                      <div 
                        className="h-2 rounded-full overflow-hidden"
                        style={{ background: 'var(--bg-secondary)' }}
                      >
                        <div 
                          className="h-full rounded-full transition-all duration-500"
                          style={{ 
                            width: `${pct}%`,
                            background: 'linear-gradient(90deg, #f97316, #fb923c)'
                          }} 
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="text-center py-10" style={{ color: 'var(--text-muted)' }}>
                No active merges
              </div>
            )}
          </div>
        </div>
      </div>
      )}
    </div>
  );
};

export default SystemOverview;
