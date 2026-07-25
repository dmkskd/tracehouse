import { describe, expect, it } from 'vitest';
import {
  isClickHouseVersionAtLeast,
  parseClickHouseVersion,
} from '../clickhouse-version.js';

describe('ClickHouse version utilities', () => {
  it('parses release and build components', () => {
    expect(parseClickHouseVersion('26.7.1.1315')).toEqual({
      major: 26,
      minor: 7,
      patch: 1,
    });
  });

  it('compares major, minor, and patch versions', () => {
    expect(isClickHouseVersionAtLeast('26.7.1.1315', 26, 7)).toBe(true);
    expect(isClickHouseVersionAtLeast('26.7.0', 26, 7, 1)).toBe(false);
    expect(isClickHouseVersionAtLeast('27.1.0', 26, 7)).toBe(true);
    expect(isClickHouseVersionAtLeast('26.6.9', 26, 7)).toBe(false);
  });

  it('fails closed for unknown versions', () => {
    expect(parseClickHouseVersion('unknown')).toBeNull();
    expect(isClickHouseVersionAtLeast('unknown', 26, 7)).toBe(false);
  });
});
