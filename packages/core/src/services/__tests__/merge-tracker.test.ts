import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter, TaggedQuery } from '../../adapters/types.js';
import { MergeTracker } from '../merge-tracker.js';

class MockAdapter implements IClickHouseAdapter {
  public queries: string[] = [];

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    return [];
  }
}

describe('MergeTracker active merges', () => {
  it('applies a sanitized server-side result limit', async () => {
    const adapter = new MockAdapter();
    const tracker = new MergeTracker(adapter);

    await tracker.getActiveMerges(undefined, undefined, 100.9);

    expect(adapter.queries).toHaveLength(1);
    expect(adapter.queries[0]).toContain('FROM {{cluster_aware:system.merges}}');
    expect(adapter.queries[0]).toContain('ORDER BY elapsed DESC');
    expect(adapter.queries[0]).toContain('LIMIT 100');
  });
});
