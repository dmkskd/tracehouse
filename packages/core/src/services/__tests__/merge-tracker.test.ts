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

describe('MergeTracker UTC custom ranges', () => {
  it('uses explicit UTC bounds for merge history', async () => {
    const adapter = new MockAdapter();
    const tracker = new MergeTracker(adapter);

    await tracker.getMergeHistory({
      timeRange: 'CUSTOM:2026-07-27T13:13:00.000Z,2026-07-27T14:05:00.000Z',
    });

    const historyQuery = adapter.queries.find(query => query.includes('system.part_log'));
    expect(historyQuery).toContain(
      "event_time >= toDateTime('2026-07-27 13:13:00', 'UTC')",
    );
    expect(historyQuery).toContain(
      "event_time <= toDateTime('2026-07-27 14:05:00', 'UTC')",
    );
  });

  it('pushes multi-value database, table, and category filters before LIMIT', async () => {
    const adapter = new MockAdapter();
    const tracker = new MergeTracker(adapter);

    await tracker.getMergeHistory({
      database: ['db_a', "db_'b"],
      table: ['table_a', 'table_b'],
      category: ['TTLDelete', 'Mutation'],
      limit: 25,
    });

    const sql = adapter.queries.find(query => query.includes('system.part_log'))!;
    expect(sql).toContain("database IN ('db_a', 'db_\\'b')");
    expect(sql).toContain("table IN ('table_a', 'table_b')");
    expect(sql).toContain("merge_reason IN ('TTLDeleteMerge', 'TTLDropMerge', 'TTLMerge')");
    expect(sql).toContain("event_type = 'MutatePart'");
    expect(sql.indexOf('database IN')).toBeLessThan(sql.indexOf('ORDER BY'));
    expect(sql).toContain('LIMIT 25');
  });

  it('pushes multi-value database and table filters into mutation history', async () => {
    const adapter = new MockAdapter();
    const tracker = new MergeTracker(adapter);

    await tracker.getMutationHistory({
      database: ['db_a', 'db_b'],
      table: ['table_a', 'table_b'],
    });

    const sql = adapter.queries.find(query => query.includes('system.mutations'))!;
    expect(sql).toContain("WHERE database IN ('db_a', 'db_b')");
    expect(sql).toContain("AND table IN ('table_a', 'table_b')");
  });

  it('uses explicit UTC bounds for mutation history', async () => {
    const adapter = new MockAdapter();
    const tracker = new MergeTracker(adapter);

    await tracker.getMutationHistory({
      timeRange: 'CUSTOM:2026-01-27T14:13:00.000Z,2026-01-27T15:05:00.000Z',
    });

    const historyQuery = adapter.queries.find(query => query.includes('system.mutations'));
    expect(historyQuery).toContain(
      "create_time >= toDateTime('2026-01-27 14:13:00', 'UTC')",
    );
    expect(historyQuery).toContain(
      "create_time <= toDateTime('2026-01-27 15:05:00', 'UTC')",
    );
  });

  it('rejects timezone-ambiguous custom ranges at the core service boundary', async () => {
    const adapter = new MockAdapter();
    const tracker = new MergeTracker(adapter);

    await expect(tracker.getMergeHistory({
      timeRange: 'CUSTOM:2026-07-27T14:13,2026-07-27T15:05',
    })).rejects.toThrow(
      'MergeTracker requires CUSTOM:start,end with explicit Z or UTC offsets',
    );
    expect(adapter.queries).toHaveLength(0);
  });

  it('rejects reversed canonical custom ranges', async () => {
    const adapter = new MockAdapter();
    const tracker = new MergeTracker(adapter);

    await expect(tracker.getMutationHistory({
      timeRange: 'CUSTOM:2026-07-27T15:05:00Z,2026-07-27T14:13:00Z',
    })).rejects.toThrow('Invalid custom time range');
    expect(adapter.queries).toHaveLength(0);
  });
});
