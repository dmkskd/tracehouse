/**
 * Time-Series Store - Zustand store for managing historical metrics data
 * 
 * This store handles time-series data storage for trend analysis.
 * It maintains historical data points with timestamps and supports
 * configurable retention periods.
 * 
 */

import { create } from 'zustand';
import type { ServerMetrics } from './metricsStore';

/**
 * A single data point in the time series
 */
export interface TimeSeriesDataPoint {
  timestamp: number; // Unix timestamp in milliseconds
  metrics: ServerMetrics;
}

/**
 * Configuration for time-series data retention
 */
export interface TimeSeriesConfig {
  /** Maximum number of data points to retain */
  maxDataPoints: number;
  /** Maximum age of data points in milliseconds (e.g., 5 minutes = 300000) */
  maxAgeMs: number;
  /** Minimum interval between data points in milliseconds */
  minIntervalMs: number;
}

/**
 * Default configuration values
 * - Keep last 60 data points (5 minutes at 5-second intervals)
 * - Maximum age of 5 minutes
 * - Minimum 1 second between data points
 */
export const DEFAULT_TIME_SERIES_CONFIG: TimeSeriesConfig = {
  maxDataPoints: 60,
  maxAgeMs: 5 * 60 * 1000, // 5 minutes
  minIntervalMs: 1000, // 1 second
};

/**
 * View mode for metrics display
 */
export type MetricsViewMode = 'snapshot' | 'trend' | 'map';

/**
 * Available metrics for trend visualization
 */
export type TrendMetricType = 
  | 'cpu_usage' 
  | 'memory_percentage' 
  | 'disk_read_rate'   // bytes per second
  | 'disk_write_rate'  // bytes per second
  | 'network_send_rate'  // bytes per second
  | 'network_recv_rate'; // bytes per second

/**
 * Processed data point for chart rendering
 */
export interface ChartDataPoint {
  timestamp: number;
  time: string; // Formatted time string for display
  cpu_usage: number;
  memory_percentage: number;
  memory_used: number;
  memory_total: number;
  disk_read_rate: number;   // bytes per second
  disk_write_rate: number;  // bytes per second
  network_send_rate: number; // bytes per second
  network_recv_rate: number; // bytes per second
}

/**
 * Filter data points by age
 * Removes data points older than maxAgeMs from the current time
 */
export function filterByAge(
  dataPoints: TimeSeriesDataPoint[],
  maxAgeMs: number,
  currentTime: number = Date.now()
): TimeSeriesDataPoint[] {
  const cutoffTime = currentTime - maxAgeMs;
  return dataPoints.filter(point => point.timestamp >= cutoffTime);
}

/**
 * Filter data points by count
 * Keeps only the most recent maxDataPoints
 */
export function filterByCount(
  dataPoints: TimeSeriesDataPoint[],
  maxDataPoints: number
): TimeSeriesDataPoint[] {
  if (dataPoints.length <= maxDataPoints) {
    return dataPoints;
  }
  return dataPoints.slice(-maxDataPoints);
}

/**
 * Check if enough time has passed since the last data point
 */
export function canAddDataPoint(
  dataPoints: TimeSeriesDataPoint[],
  minIntervalMs: number,
  currentTime: number = Date.now()
): boolean {
  if (dataPoints.length === 0) {
    return true;
  }
  const lastPoint = dataPoints[dataPoints.length - 1];
  return currentTime - lastPoint.timestamp >= minIntervalMs;
}

/**
 * Calculate memory percentage from used and total
 */
export function calculateMemoryPercentage(used: number, total: number): number {
  if (total <= 0) return 0;
  return Math.min(100, Math.max(0, (used / total) * 100));
}

/**
 * Convert time-series data points to chart-friendly format
 * Calculates rate (bytes/sec) for disk I/O from cumulative values
 */
export function toChartData(dataPoints: TimeSeriesDataPoint[]): ChartDataPoint[] {
  return dataPoints.map((point, index) => {
    // Calculate disk I/O rates from deltas
    let diskReadRate = 0;
    let diskWriteRate = 0;
    
    if (index > 0) {
      const prevPoint = dataPoints[index - 1];
      const timeDeltaSec = (point.timestamp - prevPoint.timestamp) / 1000;
      
      if (timeDeltaSec > 0) {
        const readDelta = point.metrics.disk_read_bytes - prevPoint.metrics.disk_read_bytes;
        const writeDelta = point.metrics.disk_write_bytes - prevPoint.metrics.disk_write_bytes;
        
        // Only use positive deltas (counters can reset)
        diskReadRate = readDelta > 0 ? readDelta / timeDeltaSec : 0;
        diskWriteRate = writeDelta > 0 ? writeDelta / timeDeltaSec : 0;
      }
    }
    
    return {
      timestamp: point.timestamp,
      time: new Date(point.timestamp).toLocaleTimeString([], { 
        hour: '2-digit', 
        minute: '2-digit', 
        second: '2-digit' 
      }),
      cpu_usage: point.metrics.cpu_usage,
      memory_percentage: calculateMemoryPercentage(
        point.metrics.memory_used,
        point.metrics.memory_total
      ),
      memory_used: point.metrics.memory_used,
      memory_total: point.metrics.memory_total,
      disk_read_rate: diskReadRate,
      disk_write_rate: diskWriteRate,
      // ServerMetrics doesn't include network counters yet
      network_send_rate: 0,
      network_recv_rate: 0,
    };
  });
}

/**
 * Get the latest value for a specific metric
 */
export function getLatestValue(
  dataPoints: TimeSeriesDataPoint[],
  metricType: TrendMetricType
): number | null {
  if (dataPoints.length === 0) return null;
  
  const chartData = toChartData(dataPoints);
  const latest = chartData[chartData.length - 1];
  
  switch (metricType) {
    case 'cpu_usage':
      return latest.cpu_usage;
    case 'memory_percentage':
      return latest.memory_percentage;
    case 'disk_read_rate':
      return latest.disk_read_rate;
    case 'disk_write_rate':
      return latest.disk_write_rate;
    case 'network_send_rate':
      return latest.network_send_rate;
    case 'network_recv_rate':
      return latest.network_recv_rate;
    default:
      return null;
  }
}

/**
 * Calculate statistics for a metric over the time series
 */
export interface MetricStats {
  min: number;
  max: number;
  avg: number;
  current: number;
}

export function calculateMetricStats(
  dataPoints: TimeSeriesDataPoint[],
  metricType: TrendMetricType
): MetricStats | null {
  if (dataPoints.length === 0) return null;

  const chartData = toChartData(dataPoints);
  
  const values = chartData.map(point => {
    switch (metricType) {
      case 'cpu_usage':
        return point.cpu_usage;
      case 'memory_percentage':
        return point.memory_percentage;
      case 'disk_read_rate':
        return point.disk_read_rate;
      case 'disk_write_rate':
        return point.disk_write_rate;
      case 'network_send_rate':
        return point.network_send_rate;
      case 'network_recv_rate':
        return point.network_recv_rate;
      default:
        return 0;
    }
  });

  const min = Math.min(...values);
  const max = Math.max(...values);
  const avg = values.reduce((sum, val) => sum + val, 0) / values.length;
  const current = values[values.length - 1];

  return { min, max, avg, current };
}

// Time-series store state
interface TimeSeriesState {
  // Time-series data
  dataPoints: TimeSeriesDataPoint[];
  
  // Configuration
  config: TimeSeriesConfig;
  
  // View mode
  viewMode: MetricsViewMode;
  
  // Selected metrics for trend view
  selectedMetrics: TrendMetricType[];
  
  // Actions
  addDataPoint: (metrics: ServerMetrics, timestamp?: number) => void;
  clearData: () => void;
  setConfig: (config: Partial<TimeSeriesConfig>) => void;
  resetConfig: () => void;
  setViewMode: (mode: MetricsViewMode) => void;
  toggleViewMode: () => void;
  setSelectedMetrics: (metrics: TrendMetricType[]) => void;
  toggleMetric: (metric: TrendMetricType) => void;
  
  // Computed getters
  getChartData: () => ChartDataPoint[];
  getStats: (metricType: TrendMetricType) => MetricStats | null;
  getDataPointCount: () => number;
  getTimeRange: () => { start: number; end: number } | null;
}

export const useTimeSeriesStore = create<TimeSeriesState>((set, get) => ({
  // Initial state
  dataPoints: [],
  config: { ...DEFAULT_TIME_SERIES_CONFIG },
  viewMode: 'snapshot',
  selectedMetrics: ['cpu_usage', 'memory_percentage', 'disk_read_rate', 'disk_write_rate', 'network_send_rate', 'network_recv_rate'],

  // Add a new data point with automatic cleanup
  addDataPoint: (metrics: ServerMetrics, timestamp: number = Date.now()) => {
    const { dataPoints, config } = get();
    
    // Check if we can add a new data point (respecting minimum interval)
    if (!canAddDataPoint(dataPoints, config.minIntervalMs, timestamp)) {
      return;
    }
    
    // Create new data point
    const newPoint: TimeSeriesDataPoint = { timestamp, metrics };
    
    // Add new point and apply filters
    let updatedPoints = [...dataPoints, newPoint];
    
    // Filter by age first
    updatedPoints = filterByAge(updatedPoints, config.maxAgeMs, timestamp);
    
    // Then filter by count
    updatedPoints = filterByCount(updatedPoints, config.maxDataPoints);
    
    set({ dataPoints: updatedPoints });
  },

  // Clear all data points
  clearData: () => {
    set({ dataPoints: [] });
  },

  // Update configuration
  setConfig: (newConfig: Partial<TimeSeriesConfig>) => {
    const { config, dataPoints } = get();
    const updatedConfig = { ...config, ...newConfig };
    
    // Apply new filters to existing data
    let updatedPoints = filterByAge(dataPoints, updatedConfig.maxAgeMs);
    updatedPoints = filterByCount(updatedPoints, updatedConfig.maxDataPoints);
    
    set({ 
      config: updatedConfig,
      dataPoints: updatedPoints,
    });
  },

  // Reset configuration to defaults
  resetConfig: () => {
    set({ config: { ...DEFAULT_TIME_SERIES_CONFIG } });
  },

  // Set view mode
  setViewMode: (mode: MetricsViewMode) => {
    set({ viewMode: mode });
  },

  // Toggle between snapshot and trend views
  toggleViewMode: () => {
    const { viewMode } = get();
    set({ viewMode: viewMode === 'snapshot' ? 'trend' : 'snapshot' });
  },

  // Set selected metrics for trend view
  setSelectedMetrics: (metrics: TrendMetricType[]) => {
    set({ selectedMetrics: metrics });
  },

  // Toggle a specific metric in the selection
  toggleMetric: (metric: TrendMetricType) => {
    const { selectedMetrics } = get();
    const isSelected = selectedMetrics.includes(metric);
    
    if (isSelected) {
      // Don't allow deselecting all metrics
      if (selectedMetrics.length > 1) {
        set({ selectedMetrics: selectedMetrics.filter(m => m !== metric) });
      }
    } else {
      set({ selectedMetrics: [...selectedMetrics, metric] });
    }
  },

  // Get chart-formatted data
  getChartData: () => {
    const { dataPoints } = get();
    return toChartData(dataPoints);
  },

  // Get statistics for a metric
  getStats: (metricType: TrendMetricType) => {
    const { dataPoints } = get();
    return calculateMetricStats(dataPoints, metricType);
  },

  // Get current data point count
  getDataPointCount: () => {
    return get().dataPoints.length;
  },

  // Get time range of data
  getTimeRange: () => {
    const { dataPoints } = get();
    if (dataPoints.length === 0) return null;
    
    return {
      start: dataPoints[0].timestamp,
      end: dataPoints[dataPoints.length - 1].timestamp,
    };
  },
}));

export default useTimeSeriesStore;
