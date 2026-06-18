import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter, TaggedQuery } from '../../adapters/types.js';
import { QueryAnalyzer } from '../query-analyzer.js';

class MockAdapter implements IClickHouseAdapter {
  public queries: string[] = [];

  constructor(private readonly rows: Record<string, unknown>[] = []) {}

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    return this.rows as T[];
  }
}

describe('QueryAnalyzer child query batching', () => {
  it('returns an empty map without querying ClickHouse when no ids are provided', async () => {
    const adapter = new MockAdapter();
    const analyzer = new QueryAnalyzer(adapter);

    const result = await analyzer.getSubQueriesForInitialQueries([], '2026-06-18');

    expect(result.size).toBe(0);
    expect(adapter.queries).toHaveLength(0);
  });

  it('fetches and groups child query rows by initial query id', async () => {
    const adapter = new MockAdapter([
      {
        initial_query_id: 'parent-a',
        query_id: 'child-a1',
        hostname: 'node-a',
        query_duration_ms: '120',
        memory_usage: '2048',
        read_rows: '10',
        read_bytes: '100',
        query_preview: 'SELECT 1',
        exception_code: '0',
        exception: '',
        query_start_time_microseconds: '2026-06-18 12:00:00.000001',
      },
      {
        initial_query_id: 'parent-a',
        query_id: 'child-a2',
        hostname: 'node-b',
        query_duration_ms: 80,
        memory_usage: 1024,
        read_rows: 5,
        read_bytes: 50,
        query_preview: 'SELECT 2',
        exception_code: 0,
        exception: '',
        query_start_time_microseconds: '2026-06-18 12:00:00.000002',
      },
      {
        initial_query_id: 'parent-b',
        query_id: 'child-b1',
        hostname: 'node-c',
        query_duration_ms: 20,
      },
    ]);
    const analyzer = new QueryAnalyzer(adapter);

    const result = await analyzer.getSubQueriesForInitialQueries(['parent-a', 'parent-b'], '2026-06-18');

    expect(result.get('parent-a')).toHaveLength(2);
    expect(result.get('parent-a')?.[0]).toMatchObject({
      query_id: 'child-a1',
      hostname: 'node-a',
      query_duration_ms: 120,
      memory_usage: 2048,
      read_rows: 10,
      read_bytes: 100,
    });
    expect(result.get('parent-b')).toHaveLength(1);
  });

  it('deduplicates and escapes ids in the generated batch query', async () => {
    const adapter = new MockAdapter();
    const analyzer = new QueryAnalyzer(adapter);

    await analyzer.getSubQueriesForInitialQueries(['parent-a', "parent-'b", 'parent-a'], '2026-06-18');

    expect(adapter.queries).toHaveLength(1);
    expect(adapter.queries[0]).toContain("'parent-a','parent-\\'b'");
    expect(adapter.queries[0]).toContain('event_date >= toDate');
    expect(adapter.queries[0]).toContain('row_number() OVER');
    expect(adapter.queries[0]).toContain('PARTITION BY initial_query_id');
    expect(adapter.queries[0]).toContain('WHERE rn <= 50');
    expect(adapter.queries[0]).toContain('/* source:TraceHouse:Queries:batchSubQueries */');
  });
});
