import { describe, expect, it } from 'vitest';
import { extractNumeric, isNumericValue, sortRows } from '../charts';

describe('analytics chart helpers', () => {
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
