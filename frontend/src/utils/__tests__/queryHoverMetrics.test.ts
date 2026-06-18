import { describe, expect, test } from 'vitest';
import type { QueryHistoryItem } from '../../stores/queryStore';
import {
  buildResourcePressureMetrics,
  buildScanEfficiencyMetrics,
  normalizeLog,
  resourcePressureLevel,
  resourcePressureScores,
  scanDisplay,
  selectedPct,
} from '../queryHoverMetrics';

const makeQuery = (overrides: Partial<QueryHistoryItem> = {}): QueryHistoryItem => ({
  query_id: 'q1',
  query_type: 'QueryFinish',
  query_kind: 'SELECT',
  query_start_time: '2026-06-18 10:00:00',
  query_duration_ms: 10,
  read_rows: 10,
  read_bytes: 1024,
  result_rows: 10,
  result_bytes: 1024,
  memory_usage: 1024,
  query: 'SELECT 1',
  exception: null,
  user: 'default',
  client_hostname: '',
  type: 'QueryFinish',
  efficiency_score: null,
  ...overrides,
});

describe('query hover metric calculations', () => {
  test('normalizes resource dimensions with fixed heuristic anchors', () => {
    expect(normalizeLog(0, 100, 60_000)).toBe(0);
    expect(normalizeLog(100, 100, 60_000)).toBe(0);
    expect(normalizeLog(60_000, 100, 60_000)).toBe(1);
    expect(normalizeLog(600_000, 100, 60_000)).toBe(1);
  });

  test('builds pressure scores from server/query_log fields only', () => {
    const query = makeQuery({
      query_duration_ms: 60_000,
      memory_usage: 8 * 1024 * 1024 * 1024,
      cpu_time_us: 60_000_000,
      read_bytes: 1024,
      disk_read_bytes: 10 * 1024 * 1024 * 1024,
      network_receive_bytes: 512,
      efficiency_score: 0,
    });

    const metrics = buildResourcePressureMetrics(query);

    expect(metrics.scores).toEqual({
      time: 1,
      memory: 1,
      cpu: 1,
      io: 1,
      scan: 1,
    });
    expect(metrics.cpuMs).toBe(60_000);
    expect(metrics.ioBytes).toBe(10 * 1024 * 1024 * 1024);
    expect(metrics.scanDisplay).toBe('full scan');
    expect(metrics.level).toBe('high');
  });

  test('classifies inserts and errors without React color knowledge', () => {
    expect(resourcePressureLevel(makeQuery({ query_kind: 'INSERT' }))).toBe('moderate');
    expect(resourcePressureLevel(makeQuery({ exception: 'Code: 394', type: 'ExceptionWhileProcessing' }))).toBe('high');
    expect(resourcePressureLevel(makeQuery())).toBe('low');
  });

  test('uses scan pruning as inverse scan pressure', () => {
    expect(resourcePressureScores(makeQuery({ efficiency_score: null })).scan).toBe(0);
    expect(resourcePressureScores(makeQuery({ efficiency_score: 100 })).scan).toBe(0);
    expect(resourcePressureScores(makeQuery({ efficiency_score: 25 })).scan).toBe(0.75);
    expect(scanDisplay(makeQuery({ efficiency_score: null }))).toBe('n/a');
    expect(scanDisplay(makeQuery({ efficiency_score: 0 }))).toBe('full scan');
    expect(scanDisplay(makeQuery({ efficiency_score: 84.321 }))).toBe('84.3% pruned');
  });

  test('does not classify a full scan as high pressure by itself', () => {
    expect(resourcePressureLevel(makeQuery({
      query_duration_ms: 739,
      cpu_time_us: 729_760,
      memory_usage: 459.06 * 1024 * 1024,
      read_bytes: 100.3 * 1024 * 1024,
      efficiency_score: 0,
    }))).toBe('moderate');
  });

  test('classifies full scan plus elevated resource use as high pressure', () => {
    expect(resourcePressureLevel(makeQuery({
      query_duration_ms: 15_000,
      cpu_time_us: 15_000_000,
      memory_usage: 1 * 1024 * 1024 * 1024,
      read_bytes: 2 * 1024 * 1024 * 1024,
      efficiency_score: 0,
    }))).toBe('high');
  });

  test('builds scan efficiency bars separately from display formatting', () => {
    const metrics = buildScanEfficiencyMetrics(makeQuery({
      read_bytes: 1_000,
      result_bytes: 100,
      efficiency_score: 84.3,
      selected_parts: 3,
      selected_parts_total: 5,
      selected_marks: 6,
      selected_marks_total: 23,
    }));

    expect(metrics).toMatchObject({
      readWidth: 100,
      resultWidth: 10,
      pruningDisplay: '84.3% pruned',
      pruningLevel: 'low',
      partsPct: 60,
    });
    expect(metrics.marksPct).toBeCloseTo(26.087, 2);
  });

  test('keeps zero selected counters as real server data', () => {
    expect(selectedPct(0, 10)).toBe(0);
    expect(selectedPct(undefined, 10)).toBeNull();
    expect(selectedPct(1, 0)).toBeNull();

    const metrics = buildScanEfficiencyMetrics(makeQuery({
      selected_parts: 0,
      selected_parts_total: 10,
      selected_marks: 0,
      selected_marks_total: 100,
    }));

    expect(metrics.partsPct).toBe(4);
    expect(metrics.marksPct).toBe(4);
  });
});
