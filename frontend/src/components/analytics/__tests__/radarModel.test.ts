import { describe, expect, test } from 'vitest';
import {
  buildRadar,
  buildRadarChartItems,
  buildRadarChartPlot,
  normalizeRadarValue,
  parseRangeNumber,
  pressureLevel,
  radarShapeLayout,
  shortRadarLabel,
} from '../radarModel';
import type { RadarCellStyle } from '../metaLanguage';

describe('radarModel', { tags: ['analytics'] }, () => {
  test('parses compact range units', () => {
    expect(parseRangeNumber('100k')).toBe(100_000);
    expect(parseRangeNumber('32Mi')).toBe(32 * 1024 * 1024);
    expect(parseRangeNumber('8Gi')).toBe(8 * 1024 * 1024 * 1024);
  });

  test('normalizes linear and log ranges', () => {
    expect(normalizeRadarValue(50, { low: '0', high: '100' }, 'linear')).toBe(0.5);
    expect(normalizeRadarValue(100, { low: '100', high: '60000' }, 'log')).toBe(0);
    expect(normalizeRadarValue(60000, { low: '100', high: '60000' }, 'log')).toBe(1);
    expect(normalizeRadarValue(0, { low: '100', high: '60000' }, 'log')).toBe(0);
  });

  test('builds synthetic query pressure radar from raw row columns', () => {
    const style: RadarCellStyle = {
      type: 'radar',
      radarColumn: 'shape',
      profile: 'query_pressure',
      axes: {
        time: 'query_duration_ms',
        memory: 'memory_usage',
        scan: 'scan_pressure',
      },
      ranges: {
        time: { low: '100', high: '60000' },
        memory: { low: '32Mi', high: '8Gi' },
        scan: { low: '0', high: '1' },
      },
      color: 'profile_level',
    };

    const radar = buildRadar(style, {
      query_duration_ms: 60_000,
      memory_usage: 8 * 1024 * 1024 * 1024,
      scan_pressure: 1,
      query_kind: 'Select',
      type: 'success',
    });

    expect(radar.values).toEqual([1, 1, 1]);
    expect(radar.rawValues).toEqual(['1.0 min', '8 GB', '100%']);
    expect(radar.labels).toEqual(['time', 'memory', 'scan']);
    expect(radar.color).toBe('#f85149');
  });

  test('builds radar from existing SQL array column', () => {
    const radar = buildRadar({
      type: 'radar',
      column: 'pressure_values',
      labels: 'pressure_labels',
      colorBy: 'pressure_score',
    }, {
      pressure_values: [0.2, 0.7],
      pressure_labels: ['time', 'memory'],
      pressure_score: 0.7,
    });

    expect(radar.values).toEqual([0.2, 0.7]);
    expect(radar.rawValues).toEqual(['0.20', '0.70']);
    expect(radar.labels).toEqual(['time', 'memory']);
    expect(radar.color).toBe('#d29922');
  });

  test('builds chart-level radar items from directive config and rows', () => {
    const items = buildRadarChartItems({
      labelColumn: 'short_id',
      profile: 'query_pressure',
      axes: {
        time: 'query_duration_ms',
        memory: 'memory_usage',
      },
      ranges: {
        time: { low: '100', high: '60000' },
        memory: { low: '32Mi', high: '8Gi' },
      },
      color: 'profile_level',
    }, [{
      short_id: 'abc123',
      query_duration_ms: 60_000,
      memory_usage: 32 * 1024 * 1024,
      query_kind: 'Select',
      type: 'QueryFinish',
    }]);

    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      label: 'abc123',
      labels: ['time', 'memory'],
      values: [1, 0],
      rawValues: ['1.0 min', '32 MB'],
    });
  });

  test('formats ratio pressure values without byte unit underflow', () => {
    const radar = buildRadar({
      type: 'radar',
      radarColumn: 'shape',
      axes: { memory: 'memory_pressure', cpu: 'cpu_pressure' },
      ranges: { memory: { low: '0', high: '1' }, cpu: { low: '0', high: '1' } },
    }, {
      memory_pressure: 0.2496,
      cpu_pressure: 0.28,
    });

    expect(radar.rawValues).toEqual(['24.96%', '28%']);
  });

  test('requires chart-level radar to produce exactly one row', () => {
    const config = {
      axes: { time: 'query_duration_ms' },
      ranges: { time: { low: '0', high: '100' } },
    };

    expect(buildRadarChartPlot(config, [
      { query_duration_ms: 90 },
    ])?.values).toEqual([0.9]);
    expect(buildRadarChartPlot(config, [
      { query_duration_ms: 10 },
      { query_duration_ms: 90 },
    ])).toBeNull();
  });

  test('query pressure profile classifies inserts and errors specially', () => {
    expect(pressureLevel({ query_kind: 'INSERT' }, {}, 'query_pressure')).toBe('moderate');
    expect(pressureLevel({ exception: 'Code: 1' }, {}, 'query_pressure')).toBe('high');
    expect(pressureLevel({ query_kind: 'SELECT' }, { time: 0.1, memory: 0.1, cpu: 0.1, io: 0.1, scan: 0.9 }, 'query_pressure')).toBe('moderate');
  });

  test('builds labelled SVG layout outside the radar polygon', () => {
    expect(shortRadarLabel('memory')).toBe('MEM');
    expect(shortRadarLabel('io')).toBe('I/O');
    expect(shortRadarLabel('scan')).toBe('SCAN');

    const layout = radarShapeLayout([1, 0.2, 0.5, 0.6, 1], ['time', 'memory', 'cpu', 'io', 'scan']);

    expect(layout.viewBox).toBe('-8 -8 96 96');
    expect(layout.spokes).toHaveLength(5);
    expect(layout.labels.map(label => label.label)).toEqual(['TIME', 'MEM', 'CPU', 'I/O', 'SCAN']);
    expect(layout.labels[0]).toMatchObject({ x: 40, y: 9, anchor: 'middle' });
    expect(layout.polygonPoints.split(' ')).toHaveLength(5);
    expect(layout.polygonPoints.split(' ')[0]).toBe('40,22');
  });
});
