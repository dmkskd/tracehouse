import { describe, expect, it } from 'vitest';
import { buildChartData, buildGroupedChartData, extractNumeric, isNumericValue, sortRows } from '../charts';

describe('analytics chart helpers', () => {
  it('omits missing CPU measurements while retaining measured zero CPU', () => {
    const rows = [
      { level: 'Unknown', cpu_seconds: null },
      { level: '0 → 1', cpu_seconds: 0 },
      { level: '1 → 2', cpu_seconds: 5 },
    ];
    expect(buildChartData(rows, ['level', 'cpu_seconds'], 'level', 'cpu_seconds')
      .map(({ label, value }) => ({ label, value }))).toEqual([
      { label: '0 → 1', value: 0 },
      { label: '1 → 2', value: 5 },
    ]);
  });

  it('keeps missing CPU missing in multi-column stacked data, distinct from measured zero', () => {
    const rows = [
      { tbl: 'app.events', cpu_seconds: 2, failed_cpu_seconds: null },
      { tbl: 'app.steady', cpu_seconds: 9, failed_cpu_seconds: 0 },
    ];
    expect(buildGroupedChartData(rows, 'tbl', 'cpu_seconds', undefined, ['cpu_seconds', 'failed_cpu_seconds'])
      .map(({ label, groups }) => ({ label, values: groups.map(g => g.value) }))).toEqual([
      { label: 'app.events', values: [2, null] },
      { label: 'app.steady', values: [9, 0] },
    ]);
  });

  it('treats ClickHouse readable byte strings as sortable numbers', () => {
    expect(isNumericValue('8.58 MiB')).toBe(true);
    expect(isNumericValue('85.82 MiB')).toBe(true);
    expect(extractNumeric('8.58 MiB')).toBeGreaterThan(extractNumeric('1024 B'));
  });

  it('sorts readable byte strings by size instead of lexicographically', () => {
    const rows = [
      { memory_usage: '85.82 MiB' },
      { memory_usage: '9.13 MiB' },
      { memory_usage: '1024 B' },
      { memory_usage: '1.5 GiB' },
    ];

    expect(sortRows(rows, 'memory_usage', 'asc').map(row => row.memory_usage)).toEqual([
      '1024 B',
      '9.13 MiB',
      '85.82 MiB',
      '1.5 GiB',
    ]);
  });
});
