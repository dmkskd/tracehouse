/**
 * Unit tests for the TimeSeriesStore.
 *
 * These tests verify the core functionality of the time-series store including:
 * - Data point management (add, filter, clear)
 * - Configuration management
 * - View mode toggling
 * - Metric selection
 * - Statistics calculation
 *
 */

import { describe, test, expect, beforeEach } from 'vitest';
import { act } from 'react';
import {
  useTimeSeriesStore,
  filterByAge,
  filterByCount,
  canAddDataPoint,
  calculateMemoryPercentage,
  toChartData,
  getLatestValue,
  calculateMetricStats,
  DEFAULT_TIME_SERIES_CONFIG,
  type TimeSeriesDataPoint,
  type TrendMetricType,
} from '../timeSeriesStore';
import type { ServerMetrics } from '../metricsStore';

// Helper to create mock metrics
function createMockMetrics(overrides: Partial<ServerMetrics> = {}): ServerMetrics {
  return {
    timestamp: new Date().toISOString(),
    cpu_usage: 50,
    memory_used: 4 * 1024 * 1024 * 1024, // 4GB
    memory_total: 8 * 1024 * 1024 * 1024, // 8GB
    disk_read_bytes: 1000000,
    disk_write_bytes: 500000,
    uptime_seconds: 3600,
    ...overrides,
  };
}

// Helper to create mock data points
function createMockDataPoints(count: number, startTime: number = Date.now()): TimeSeriesDataPoint[] {
  return Array.from({ length: count }, (_, i) => ({
    timestamp: startTime + i * 5000, // 5 second intervals
    metrics: createMockMetrics({ cpu_usage: 50 + i }),
  }));
}

describe('TimeSeriesStore', () => {
  beforeEach(() => {
    // Reset store state before each test
    const store = useTimeSeriesStore.getState();
    store.clearData();
    store.resetConfig();
    store.setViewMode('snapshot');
    store.setSelectedMetrics(['cpu_usage', 'memory_percentage', 'disk_read_rate', 'disk_write_rate', 'network_send_rate', 'network_recv_rate']);
  });

  describe('filterByAge', () => {
    test('filters out data points older than maxAgeMs', () => {
      const now = Date.now();
      const dataPoints: TimeSeriesDataPoint[] = [
        { timestamp: now - 400000, metrics: createMockMetrics() }, // 6.67 min ago
        { timestamp: now - 200000, metrics: createMockMetrics() }, // 3.33 min ago
        { timestamp: now - 100000, metrics: createMockMetrics() }, // 1.67 min ago
        { timestamp: now, metrics: createMockMetrics() },          // now
      ];

      const filtered = filterByAge(dataPoints, 300000, now); // 5 min max age

      expect(filtered).toHaveLength(3);
      expect(filtered[0].timestamp).toBe(now - 200000);
    });

    test('returns all points if none are too old', () => {
      const now = Date.now();
      const dataPoints: TimeSeriesDataPoint[] = [
        { timestamp: now - 60000, metrics: createMockMetrics() },
        { timestamp: now - 30000, metrics: createMockMetrics() },
        { timestamp: now, metrics: createMockMetrics() },
      ];

      const filtered = filterByAge(dataPoints, 300000, now);

      expect(filtered).toHaveLength(3);
    });

    test('returns empty array if all points are too old', () => {
      const now = Date.now();
      const dataPoints: TimeSeriesDataPoint[] = [
        { timestamp: now - 600000, metrics: createMockMetrics() },
        { timestamp: now - 500000, metrics: createMockMetrics() },
      ];

      const filtered = filterByAge(dataPoints, 300000, now);

      expect(filtered).toHaveLength(0);
    });

    test('handles empty array', () => {
      const filtered = filterByAge([], 300000);
      expect(filtered).toHaveLength(0);
    });
  });

  describe('filterByCount', () => {
    test('keeps only the most recent maxDataPoints', () => {
      const dataPoints = createMockDataPoints(10);

      const filtered = filterByCount(dataPoints, 5);

      expect(filtered).toHaveLength(5);
      expect(filtered[0]).toBe(dataPoints[5]);
      expect(filtered[4]).toBe(dataPoints[9]);
    });

    test('returns all points if count is less than max', () => {
      const dataPoints = createMockDataPoints(3);

      const filtered = filterByCount(dataPoints, 10);

      expect(filtered).toHaveLength(3);
    });

    test('handles empty array', () => {
      const filtered = filterByCount([], 10);
      expect(filtered).toHaveLength(0);
    });

    test('handles exact count match', () => {
      const dataPoints = createMockDataPoints(5);

      const filtered = filterByCount(dataPoints, 5);

      expect(filtered).toHaveLength(5);
      expect(filtered).toEqual(dataPoints);
    });
  });

  describe('canAddDataPoint', () => {
    test('returns true for empty array', () => {
      expect(canAddDataPoint([], 1000)).toBe(true);
    });

    test('returns true if enough time has passed', () => {
      const now = Date.now();
      const dataPoints: TimeSeriesDataPoint[] = [
        { timestamp: now - 2000, metrics: createMockMetrics() },
      ];

      expect(canAddDataPoint(dataPoints, 1000, now)).toBe(true);
    });

    test('returns false if not enough time has passed', () => {
      const now = Date.now();
      const dataPoints: TimeSeriesDataPoint[] = [
        { timestamp: now - 500, metrics: createMockMetrics() },
      ];

      expect(canAddDataPoint(dataPoints, 1000, now)).toBe(false);
    });

    test('returns true at exact interval boundary', () => {
      const now = Date.now();
      const dataPoints: TimeSeriesDataPoint[] = [
        { timestamp: now - 1000, metrics: createMockMetrics() },
      ];

      expect(canAddDataPoint(dataPoints, 1000, now)).toBe(true);
    });
  });

  describe('calculateMemoryPercentage', () => {
    test('calculates correct percentage', () => {
      expect(calculateMemoryPercentage(4, 8)).toBe(50);
      expect(calculateMemoryPercentage(2, 8)).toBe(25);
      expect(calculateMemoryPercentage(8, 8)).toBe(100);
    });

    test('returns 0 for zero total', () => {
      expect(calculateMemoryPercentage(4, 0)).toBe(0);
    });

    test('returns 0 for negative total', () => {
      expect(calculateMemoryPercentage(4, -8)).toBe(0);
    });

    test('clamps to 100 for values over 100%', () => {
      expect(calculateMemoryPercentage(10, 8)).toBe(100);
    });

    test('clamps to 0 for negative used', () => {
      expect(calculateMemoryPercentage(-4, 8)).toBe(0);
    });
  });

  describe('toChartData', () => {
    test('converts data points to chart format', () => {
      const now = Date.now();
      const dataPoints: TimeSeriesDataPoint[] = [
        {
          timestamp: now,
          metrics: createMockMetrics({
            cpu_usage: 75,
            memory_used: 6 * 1024 * 1024 * 1024,
            memory_total: 8 * 1024 * 1024 * 1024,
          }),
        },
      ];

      const chartData = toChartData(dataPoints);

      expect(chartData).toHaveLength(1);
      expect(chartData[0].timestamp).toBe(now);
      expect(chartData[0].cpu_usage).toBe(75);
      expect(chartData[0].memory_percentage).toBe(75);
      expect(chartData[0].time).toBeDefined();
    });

    test('handles empty array', () => {
      const chartData = toChartData([]);
      expect(chartData).toHaveLength(0);
    });
  });

  describe('getLatestValue', () => {
    test('returns latest CPU usage', () => {
      const dataPoints = createMockDataPoints(3);
      dataPoints[2].metrics.cpu_usage = 99;

      const value = getLatestValue(dataPoints, 'cpu_usage');

      expect(value).toBe(99);
    });

    test('returns latest memory percentage', () => {
      const dataPoints: TimeSeriesDataPoint[] = [
        {
          timestamp: Date.now(),
          metrics: createMockMetrics({
            memory_used: 6 * 1024 * 1024 * 1024,
            memory_total: 8 * 1024 * 1024 * 1024,
          }),
        },
      ];

      const value = getLatestValue(dataPoints, 'memory_percentage');

      expect(value).toBe(75);
    });

    test('returns null for empty array', () => {
      const value = getLatestValue([], 'cpu_usage');
      expect(value).toBeNull();
    });
  });

  describe('calculateMetricStats', () => {
    test('calculates correct statistics for CPU usage', () => {
      const dataPoints: TimeSeriesDataPoint[] = [
        { timestamp: 1, metrics: createMockMetrics({ cpu_usage: 20 }) },
        { timestamp: 2, metrics: createMockMetrics({ cpu_usage: 40 }) },
        { timestamp: 3, metrics: createMockMetrics({ cpu_usage: 60 }) },
        { timestamp: 4, metrics: createMockMetrics({ cpu_usage: 80 }) },
      ];

      const stats = calculateMetricStats(dataPoints, 'cpu_usage');

      expect(stats).not.toBeNull();
      expect(stats!.min).toBe(20);
      expect(stats!.max).toBe(80);
      expect(stats!.avg).toBe(50);
      expect(stats!.current).toBe(80);
    });

    test('returns null for empty array', () => {
      const stats = calculateMetricStats([], 'cpu_usage');
      expect(stats).toBeNull();
    });

    test('handles single data point', () => {
      const dataPoints: TimeSeriesDataPoint[] = [
        { timestamp: 1, metrics: createMockMetrics({ cpu_usage: 50 }) },
      ];

      const stats = calculateMetricStats(dataPoints, 'cpu_usage');

      expect(stats).not.toBeNull();
      expect(stats!.min).toBe(50);
      expect(stats!.max).toBe(50);
      expect(stats!.avg).toBe(50);
      expect(stats!.current).toBe(50);
    });
  });

  describe('Store Actions', () => {
    test('addDataPoint adds a new data point', () => {
      const metrics = createMockMetrics();

      act(() => {
        useTimeSeriesStore.getState().addDataPoint(metrics);
      });

      const state = useTimeSeriesStore.getState();
      expect(state.dataPoints).toHaveLength(1);
      expect(state.dataPoints[0].metrics).toEqual(metrics);
    });

    test('addDataPoint respects minimum interval', () => {
      const metrics = createMockMetrics();
      const now = Date.now();

      act(() => {
        useTimeSeriesStore.getState().addDataPoint(metrics, now);
        useTimeSeriesStore.getState().addDataPoint(metrics, now + 500); // Too soon
      });

      const state = useTimeSeriesStore.getState();
      expect(state.dataPoints).toHaveLength(1);
    });

    test('addDataPoint filters by age', () => {
      const now = Date.now();
      const oldTime = now - 10 * 60 * 1000; // 10 minutes ago

      act(() => {
        // Add old data point directly to state
        useTimeSeriesStore.setState({
          dataPoints: [{ timestamp: oldTime, metrics: createMockMetrics() }],
        });
        // Add new data point
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics(), now);
      });

      // Old point should be filtered out
      const state = useTimeSeriesStore.getState();
      expect(state.dataPoints).toHaveLength(1);
      expect(state.dataPoints[0].timestamp).toBe(now);
    });

    test('addDataPoint filters by count', () => {
      // Set a small max data points
      act(() => {
        useTimeSeriesStore.getState().setConfig({ maxDataPoints: 3, minIntervalMs: 0 });
      });

      // Add more than max data points
      act(() => {
        for (let i = 0; i < 5; i++) {
          useTimeSeriesStore.getState().addDataPoint(createMockMetrics(), Date.now() + i * 1000);
        }
      });

      const state = useTimeSeriesStore.getState();
      expect(state.dataPoints).toHaveLength(3);
    });

    test('clearData removes all data points', () => {
      act(() => {
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics());
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics(), Date.now() + 2000);
        useTimeSeriesStore.getState().clearData();
      });

      const state = useTimeSeriesStore.getState();
      expect(state.dataPoints).toHaveLength(0);
    });

    test('setConfig updates configuration', () => {
      act(() => {
        useTimeSeriesStore.getState().setConfig({ maxDataPoints: 100, maxAgeMs: 600000 });
      });

      const state = useTimeSeriesStore.getState();
      expect(state.config.maxDataPoints).toBe(100);
      expect(state.config.maxAgeMs).toBe(600000);
      expect(state.config.minIntervalMs).toBe(DEFAULT_TIME_SERIES_CONFIG.minIntervalMs);
    });

    test('resetConfig restores defaults', () => {
      act(() => {
        useTimeSeriesStore.getState().setConfig({ maxDataPoints: 100 });
        useTimeSeriesStore.getState().resetConfig();
      });

      const state = useTimeSeriesStore.getState();
      expect(state.config).toEqual(DEFAULT_TIME_SERIES_CONFIG);
    });

    test('setViewMode changes view mode', () => {
      act(() => {
        useTimeSeriesStore.getState().setViewMode('trend');
      });

      const state = useTimeSeriesStore.getState();
      expect(state.viewMode).toBe('trend');
    });

    test('toggleViewMode switches between modes', () => {
      act(() => {
        useTimeSeriesStore.getState().toggleViewMode();
      });
      expect(useTimeSeriesStore.getState().viewMode).toBe('trend');

      act(() => {
        useTimeSeriesStore.getState().toggleViewMode();
      });
      expect(useTimeSeriesStore.getState().viewMode).toBe('snapshot');
    });

    test('setSelectedMetrics updates selected metrics', () => {
      const newMetrics: TrendMetricType[] = ['disk_read_rate', 'disk_write_rate'];

      act(() => {
        useTimeSeriesStore.getState().setSelectedMetrics(newMetrics);
      });

      const state = useTimeSeriesStore.getState();
      expect(state.selectedMetrics).toEqual(newMetrics);
    });

    test('toggleMetric adds metric if not selected', () => {
      act(() => {
        useTimeSeriesStore.getState().setSelectedMetrics(['cpu_usage']);
        useTimeSeriesStore.getState().toggleMetric('memory_percentage');
      });

      const state = useTimeSeriesStore.getState();
      expect(state.selectedMetrics).toContain('cpu_usage');
      expect(state.selectedMetrics).toContain('memory_percentage');
    });

    test('toggleMetric removes metric if selected', () => {
      act(() => {
        useTimeSeriesStore.getState().setSelectedMetrics(['cpu_usage', 'memory_percentage']);
        useTimeSeriesStore.getState().toggleMetric('memory_percentage');
      });

      const state = useTimeSeriesStore.getState();
      expect(state.selectedMetrics).toContain('cpu_usage');
      expect(state.selectedMetrics).not.toContain('memory_percentage');
    });

    test('toggleMetric does not remove last metric', () => {
      act(() => {
        useTimeSeriesStore.getState().setSelectedMetrics(['cpu_usage']);
        useTimeSeriesStore.getState().toggleMetric('cpu_usage');
      });

      const state = useTimeSeriesStore.getState();
      expect(state.selectedMetrics).toContain('cpu_usage');
      expect(state.selectedMetrics).toHaveLength(1);
    });
  });

  describe('Store Getters', () => {
    test('getChartData returns formatted chart data', () => {
      act(() => {
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics({ cpu_usage: 50 }));
      });

      const chartData = useTimeSeriesStore.getState().getChartData();

      expect(chartData).toHaveLength(1);
      expect(chartData[0].cpu_usage).toBe(50);
    });

    test('getStats returns statistics for metric', () => {
      act(() => {
        useTimeSeriesStore.getState().setConfig({ minIntervalMs: 0 });
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics({ cpu_usage: 20 }), Date.now());
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics({ cpu_usage: 80 }), Date.now() + 1000);
      });

      const stats = useTimeSeriesStore.getState().getStats('cpu_usage');

      expect(stats).not.toBeNull();
      expect(stats!.min).toBe(20);
      expect(stats!.max).toBe(80);
    });

    test('getDataPointCount returns correct count', () => {
      expect(useTimeSeriesStore.getState().getDataPointCount()).toBe(0);

      act(() => {
        useTimeSeriesStore.getState().setConfig({ minIntervalMs: 0 });
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics(), Date.now());
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics(), Date.now() + 1000);
      });

      expect(useTimeSeriesStore.getState().getDataPointCount()).toBe(2);
    });

    test('getTimeRange returns null for empty data', () => {
      expect(useTimeSeriesStore.getState().getTimeRange()).toBeNull();
    });

    test('getTimeRange returns correct range', () => {
      const start = Date.now();
      const end = start + 5000;

      act(() => {
        useTimeSeriesStore.getState().setConfig({ minIntervalMs: 0 });
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics(), start);
        useTimeSeriesStore.getState().addDataPoint(createMockMetrics(), end);
      });

      const range = useTimeSeriesStore.getState().getTimeRange();

      expect(range).not.toBeNull();
      expect(range!.start).toBe(start);
      expect(range!.end).toBe(end);
    });
  });
});
