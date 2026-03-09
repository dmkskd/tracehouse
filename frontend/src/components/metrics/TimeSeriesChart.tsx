/**
 * TimeSeriesChart - Component for displaying time-series metrics trends
 * 
 * This component renders line charts for visualizing metric trends over time.
 * It supports multiple metrics on the same chart with responsive sizing.
 * 
 */

import React, { useMemo } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import type { ChartDataPoint, TrendMetricType, MetricStats } from '../../stores/timeSeriesStore';
import { truncateHostname } from '@tracehouse/core';
import { formatBytes } from '../../utils/formatters';

/**
 * Metric configuration for chart rendering
 */
interface MetricConfig {
  key: TrendMetricType;
  label: string;
  color: string;
  unit: string;
  yAxisId: 'left' | 'right';
  formatter: (value: number) => string;
}

/**
 * Metric configurations
 */
const METRIC_CONFIGS: Record<TrendMetricType, MetricConfig> = {
  cpu_usage: {
    key: 'cpu_usage',
    label: 'CPU Usage',
    color: '#3B82F6', // blue-500
    unit: '%',
    yAxisId: 'left',
    formatter: (value: number) => `${value.toFixed(1)}%`,
  },
  memory_percentage: {
    key: 'memory_percentage',
    label: 'Memory Usage',
    color: '#8B5CF6', // purple-500
    unit: '%',
    yAxisId: 'left',
    formatter: (value: number) => `${value.toFixed(1)}%`,
  },
  disk_read_rate: {
    key: 'disk_read_rate',
    label: 'Disk Read',
    color: '#10B981', // green-500
    unit: 'bytes/s',
    yAxisId: 'right',
    formatter: (value: number) => `${formatBytes(value)}/s`,
  },
  disk_write_rate: {
    key: 'disk_write_rate',
    label: 'Disk Write',
    color: '#F59E0B', // yellow-500
    unit: 'bytes/s',
    yAxisId: 'right',
    formatter: (value: number) => `${formatBytes(value)}/s`,
  },
  network_send_rate: {
    key: 'network_send_rate',
    label: 'Net Send',
    color: '#EC4899', // pink-500
    unit: 'bytes/s',
    yAxisId: 'right',
    formatter: (value: number) => `${formatBytes(value)}/s`,
  },
  network_recv_rate: {
    key: 'network_recv_rate',
    label: 'Net Recv',
    color: '#14B8A6', // teal-500
    unit: 'bytes/s',
    yAxisId: 'right',
    formatter: (value: number) => `${formatBytes(value)}/s`,
  },
};

/**
 * Props for TimeSeriesChart component
 */
export interface TimeSeriesChartProps {
  /** Chart data points */
  data: ChartDataPoint[];
  /** Metrics to display */
  selectedMetrics: TrendMetricType[];
  /** Chart height in pixels */
  height?: number;
  /** Show grid lines */
  showGrid?: boolean;
  /** Show legend */
  showLegend?: boolean;
  /** Animation duration in ms (0 to disable) */
  animationDuration?: number;
  /** Statistics for each metric */
  stats?: Partial<Record<TrendMetricType, MetricStats | null>>;
  /** Callback to toggle a metric */
  onToggleMetric?: (metric: TrendMetricType) => void;
  /** Host names for multi-host mode. When set, data keys are metric__host */
  hosts?: string[];
}

/**
 * Shade multipliers per host index — applied to the metric's base color.
 * Host 0 gets the full color, subsequent hosts get lighter/darker variants.
 */
const HOST_SHADE_FACTORS = [1.0, 0.6, 1.4, 0.4, 1.2, 0.8, 1.6, 0.5];

/** Dash patterns per host index so lines are distinguishable even in mono */
const HOST_DASHES = ['', '8 4', '4 4', '2 4', '8 2 2 2', '6 3', '4 2 4 2', '10 4 2 4'];

/** Convert hex color to RGB, apply a shade factor, return hex */
function shadeColor(hex: string, factor: number): string {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const clamp = (v: number) => Math.min(255, Math.max(0, Math.round(v)));
  if (factor > 1) {
    // Lighten: blend toward white
    const t = factor - 1;
    return `#${clamp(r + (255 - r) * t).toString(16).padStart(2, '0')}${clamp(g + (255 - g) * t).toString(16).padStart(2, '0')}${clamp(b + (255 - b) * t).toString(16).padStart(2, '0')}`;
  }
  // Darken: scale down
  return `#${clamp(r * factor).toString(16).padStart(2, '0')}${clamp(g * factor).toString(16).padStart(2, '0')}${clamp(b * factor).toString(16).padStart(2, '0')}`;
}

/**
 * Custom tooltip component
 */
const CustomTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{
    dataKey: string;
    value: number;
    color: string;
  }>;
  label?: string;
}> = ({ active, payload, label }) => {
  if (!active || !payload || payload.length === 0) {
    return null;
  }

  return (
    <div 
      style={{ 
        background: 'var(--bg-secondary)', 
        border: '1px solid var(--border-primary)',
        borderRadius: '8px',
        padding: '12px',
        boxShadow: '0 4px 12px rgba(0,0,0,0.3)'
      }}
    >
      <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '8px' }}>
        {label}
      </p>
      {payload.map((entry, index) => {
        // Handle multi-host keys like "cpu_usage__hostname"
        const parts = entry.dataKey.split('__');
        const metricKey = parts[0];
        const hostName = parts[1] || null;
        const config = Object.values(METRIC_CONFIGS).find(
          c => c.key === metricKey
        );
        if (!config) return null;
        
        return (
          <div key={index} style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '12px', marginBottom: '4px' }}>
            <div
              style={{ 
                width: '8px', 
                height: '8px', 
                borderRadius: '50%', 
                backgroundColor: entry.color,
                flexShrink: 0
              }}
            />
            <span style={{ color: 'var(--text-secondary)' }}>
              {config.label}{hostName ? ` (${truncateHostname(hostName)})` : ''}:
            </span>
            <span style={{ color: 'var(--text-primary)', fontWeight: 500 }}>
              {config.formatter(entry.value)}
            </span>
          </div>
        );
      })}
    </div>
  );
};

/**
 * Statistics display component - compact inline format
 */
const StatsDisplay: React.FC<{
  metric: TrendMetricType;
  stats: MetricStats;
  isSelected: boolean;
  onToggle: () => void;
}> = ({ metric, stats, isSelected, onToggle }) => {
  const config = METRIC_CONFIGS[metric];
  const rangeText = stats.min === stats.max
    ? config.formatter(stats.min)
    : `${config.formatter(stats.min)} – ${config.formatter(stats.max)}`;
  
  return (
    <button
      onClick={onToggle}
      style={{ 
        display: 'flex', 
        alignItems: 'center', 
        gap: '8px', 
        fontSize: '12px',
        padding: '6px 12px',
        borderRadius: '6px',
        border: 'none',
        cursor: 'pointer',
        background: isSelected ? 'var(--bg-tertiary)' : 'transparent',
        opacity: isSelected ? 1 : 0.4,
        transition: 'all 0.2s',
      }}
    >
      <div
        style={{ 
          width: '8px', 
          height: '8px', 
          borderRadius: '50%', 
          backgroundColor: config.color,
          flexShrink: 0
        }}
      />
      <span style={{ color: 'var(--text-primary)', fontWeight: 500 }}>{config.label}</span>
      <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>
        {rangeText}
      </span>
    </button>
  );
};

/**
 * TimeSeriesChart component
 */
export const TimeSeriesChart: React.FC<TimeSeriesChartProps> = ({
  data,
  selectedMetrics,
  height = 300,
  showGrid = true,
  showLegend = false,
  animationDuration = 0,
  stats,
  onToggleMetric,
  hosts,
}) => {
  const isMultiHost = hosts && hosts.length > 1;
  // Determine if we need the right Y-axis (for bytes/s metrics)
  const needsRightAxis = useMemo(() => {
    return selectedMetrics.some(
      m => METRIC_CONFIGS[m]?.yAxisId === 'right'
    );
  }, [selectedMetrics]);

  // Determine if we need the left Y-axis (for percentage metrics)
  const needsLeftAxis = useMemo(() => {
    return selectedMetrics.some(
      m => METRIC_CONFIGS[m]?.yAxisId === 'left'
    );
  }, [selectedMetrics]);

  // Calculate Y-axis domain for percentage metrics
  const percentageDomain = useMemo(() => {
    if (!needsLeftAxis) return [0, 100];
    
    const percentageMetrics = selectedMetrics.filter(
      m => METRIC_CONFIGS[m]?.yAxisId === 'left'
    );
    
    if (percentageMetrics.length === 0 || data.length === 0) {
      return [0, 100];
    }

    let maxValue = 0;
    data.forEach(point => {
      const row = point as unknown as Record<string, unknown>;
      percentageMetrics.forEach(metric => {
        if (isMultiHost) {
          // Check per-host keys like cpu_usage__host1
          for (const host of hosts!) {
            const val = Number(row[`${metric}__${host}`] ?? 0);
            if (val > maxValue) maxValue = val;
          }
        } else {
          const value = point[metric] as number;
          if (value > maxValue) maxValue = value;
        }
      });
    });

    // Add some padding and round up
    return [0, Math.min(100, Math.ceil(maxValue * 1.1 / 10) * 10)];
  }, [data, selectedMetrics, needsLeftAxis, isMultiHost, hosts]);

  // Empty state
  if (data.length === 0) {
    return (
      <div 
        className="flex items-center justify-center bg-gray-50 dark:bg-gray-800 rounded-lg"
        style={{ height }}
      >
        <div className="text-center">
          <div className="text-2xl mb-2 font-light text-gray-400">--</div>
          <p className="text-gray-500 dark:text-gray-400">
            No data available yet
          </p>
          <p className="text-sm text-gray-400 dark:text-gray-500">
            Data will appear as metrics are collected
          </p>
        </div>
      </div>
    );
  }

  // Insufficient data state
  if (data.length < 2) {
    return (
      <div 
        className="flex items-center justify-center bg-gray-50 dark:bg-gray-800 rounded-lg"
        style={{ height }}
      >
        <div className="text-center">
          <div className="animate-spin inline-block w-8 h-8 border-3 border-gray-400 border-t-gray-600 rounded-full mb-2"></div>
          <p className="text-gray-500 dark:text-gray-400">
            Collecting data...
          </p>
          <p className="text-sm text-gray-400 dark:text-gray-500">
            Need at least 2 data points to show trends
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {/* Chart */}
      <ResponsiveContainer width="100%" height={height}>
        <LineChart
          data={data}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          {showGrid && (
            <CartesianGrid 
              strokeDasharray="3 3" 
              stroke="#E5E7EB"
              className="dark:stroke-gray-700"
            />
          )}
          
          <XAxis
            dataKey="time"
            tick={{ fontSize: 12 }}
            tickLine={{ stroke: '#9CA3AF' }}
            axisLine={{ stroke: '#9CA3AF' }}
            className="text-gray-600 dark:text-gray-400"
          />
          
          {needsLeftAxis && (
            <YAxis
              yAxisId="left"
              domain={percentageDomain}
              tick={{ fontSize: 12 }}
              tickLine={{ stroke: '#9CA3AF' }}
              axisLine={{ stroke: '#9CA3AF' }}
              tickFormatter={(value) => `${value}%`}
              className="text-gray-600 dark:text-gray-400"
            />
          )}
          
          {needsRightAxis && (
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 12 }}
              tickLine={{ stroke: '#9CA3AF' }}
              axisLine={{ stroke: '#9CA3AF' }}
              tickFormatter={formatBytes}
              className="text-gray-600 dark:text-gray-400"
            />
          )}
          
          <Tooltip content={<CustomTooltip />} />
          
          {showLegend && (
            <Legend
              wrapperStyle={{ paddingTop: '10px' }}
              formatter={(value) => {
                const config = Object.values(METRIC_CONFIGS).find(
                  c => c.key === value
                );
                return config?.label || value;
              }}
            />
          )}
          
          {selectedMetrics.map(metric => {
            const config = METRIC_CONFIGS[metric];
            if (isMultiHost) {
              // Render one line per host per metric — same base color, different shades
              return hosts!.map((host, hostIdx) => (
                <Line
                  key={`${metric}__${host}`}
                  type="monotone"
                  dataKey={`${metric}__${host}`}
                  name={`${metric}__${host}`}
                  stroke={shadeColor(config.color, HOST_SHADE_FACTORS[hostIdx % HOST_SHADE_FACTORS.length])}
                  strokeWidth={2}
                  strokeDasharray={HOST_DASHES[hostIdx % HOST_DASHES.length]}
                  dot={false}
                  activeDot={{ r: 4, strokeWidth: 2 }}
                  yAxisId={config.yAxisId}
                  animationDuration={animationDuration}
                  connectNulls
                />
              ));
            }
            return (
              <Line
                key={metric}
                type="monotone"
                dataKey={metric}
                name={metric}
                stroke={config.color}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, strokeWidth: 2 }}
                yAxisId={config.yAxisId}
                animationDuration={animationDuration}
                connectNulls
              />
            );
          })}
        </LineChart>
      </ResponsiveContainer>

      {/* Statistics - clickable to toggle metrics */}
      {stats && onToggleMetric && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', padding: '8px 0' }}>
          {(['cpu_usage', 'memory_percentage', 'disk_read_rate', 'disk_write_rate', 'network_send_rate', 'network_recv_rate'] as TrendMetricType[]).map(metric => {
            const metricStats = stats[metric];
            if (!metricStats) return null;
            const isSelected = selectedMetrics.includes(metric);
            return (
              <StatsDisplay
                key={metric}
                metric={metric}
                stats={metricStats}
                isSelected={isSelected}
                onToggle={() => {
                  // Don't allow deselecting all
                  if (isSelected && selectedMetrics.length === 1) return;
                  onToggleMetric(metric);
                }}
              />
            );
          })}
        </div>
      )}
    </div>
  );
};

/**
 * Props for MetricSelector component
 */
export interface MetricSelectorProps {
  /** Currently selected metrics */
  selectedMetrics: TrendMetricType[];
  /** Callback when metric selection changes */
  onToggleMetric: (metric: TrendMetricType) => void;
}

/**
 * MetricSelector component for choosing which metrics to display
 */
export const MetricSelector: React.FC<MetricSelectorProps> = ({
  selectedMetrics,
  onToggleMetric,
}) => {
  // All available metrics
  const allMetrics: TrendMetricType[] = [
    'cpu_usage',
    'memory_percentage',
    'disk_read_rate',
    'disk_write_rate',
    'network_send_rate',
    'network_recv_rate',
  ];

  return (
    <div className="flex flex-wrap gap-2">
      {allMetrics.map(metric => {
        const config = METRIC_CONFIGS[metric];
        const isSelected = selectedMetrics.includes(metric);
        
        return (
          <button
            key={metric}
            onClick={() => onToggleMetric(metric)}
            className={`
              flex items-center space-x-2 px-3 py-1.5 rounded-full text-sm font-medium
              transition-colors duration-200
              ${isSelected
                ? 'bg-gray-800 text-white dark:bg-gray-200 dark:text-gray-800'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600'
              }
            `}
          >
            <div
              className="w-2 h-2 rounded-full"
              style={{ backgroundColor: config.color }}
            />
            <span>{config.label}</span>
          </button>
        );
      })}
    </div>
  );
};

/**
 * Props for ViewModeToggle component
 */
export interface ViewModeToggleProps {
  /** Current view mode */
  viewMode: 'snapshot' | 'trend';
  /** Callback when view mode changes */
  onToggle: () => void;
}

/**
 * ViewModeToggle component for switching between snapshot and trend views
 */
export const ViewModeToggle: React.FC<ViewModeToggleProps> = ({
  viewMode,
  onToggle,
}) => {
  return (
    <div className="flex items-center bg-gray-100 dark:bg-gray-700 rounded-lg p-1">
      <button
        onClick={viewMode === 'trend' ? onToggle : undefined}
        className={`
          px-3 py-1.5 rounded-md text-sm font-medium transition-colors duration-200
          ${viewMode === 'snapshot'
            ? 'bg-white dark:bg-gray-600 text-gray-800 dark:text-white shadow-sm'
            : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }
        `}
      >
        Snapshot
      </button>
      <button
        onClick={viewMode === 'snapshot' ? onToggle : undefined}
        className={`
          px-3 py-1.5 rounded-md text-sm font-medium transition-colors duration-200
          ${viewMode === 'trend'
            ? 'bg-white dark:bg-gray-600 text-gray-800 dark:text-white shadow-sm'
            : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }
        `}
      >
        📈 Trends
      </button>
    </div>
  );
};

export default TimeSeriesChart;
