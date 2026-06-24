import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter, TaggedQuery } from '../../adapters/types.js';
import { QueryAnalyzer } from '../query-analyzer.js';

class MockAdapter implements IClickHouseAdapter {
  public queries: string[] = [];
  public responseQueue: Record<string, unknown>[][] = [];

  constructor(private readonly rows: Record<string, unknown>[] = []) {}

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    if (this.responseQueue.length > 0) {
      return this.responseQueue.shift() as T[];
    }
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

describe('QueryAnalyzer distributed topology async insert links', () => {
  it('loads AsyncInsertFlush query_log rows via asynchronous_insert_log.flush_query_id', async () => {
    const adapter = new MockAdapter();
    adapter.responseQueue = [
      [
        {
          query_id: 'insert-root',
          initial_query_id: 'insert-root',
          is_initial_query: 1,
          normalized_query_hash: '111',
          hostname: 'node-a',
          query_kind: 'Insert',
          query_start_time_microseconds: '2026-06-18 12:00:00.000000',
          query_duration_ms: 2,
          memory_usage: 1000,
          written_rows: 10,
          written_bytes: 100,
          tables: ['db.dist_table'],
          query_preview: 'INSERT INTO db.dist_table VALUES',
          ProfileEvents: { InsertedRows: 10 },
        },
      ],
      [],
      [],
      [
        {
          event_time_microseconds: '2026-06-18 12:00:00.001000',
          hostname: 'node-a',
          query_id: 'insert-root',
          flush_query_id: 'flush-1',
          database: 'db',
          table: 'local_table',
          status: 'Ok',
          exception: '',
          rows: 10,
          bytes: 100,
        },
      ],
      [
        {
          query_id: 'flush-1',
          initial_query_id: 'flush-1',
          is_initial_query: 1,
          normalized_query_hash: '222',
          hostname: 'node-a',
          query_kind: 'AsyncInsertFlush',
          query_start_time_microseconds: '2026-06-18 12:00:00.002000',
          query_duration_ms: 3,
          memory_usage: 1200,
          written_rows: 10,
          written_bytes: 100,
          tables: ['db.local_table'],
          query_preview: 'AsyncInsertFlush db.local_table',
          ProfileEvents: { AsyncInsertRows: 10 },
        },
      ],
      [],
    ];
    const analyzer = new QueryAnalyzer(adapter);

    const topology = await analyzer.getDistributedTopology('insert-root', '2026-06-18');

    expect(topology.nodes.map(node => [node.queryId, node.role])).toEqual([
      ['insert-root', 'insert_client'],
      ['flush-1', 'async_insert_flush'],
    ]);
    expect(topology.asyncInsertLinks).toHaveLength(1);
    expect(topology.asyncInsertLinks[0]).toMatchObject({
      queryId: 'insert-root',
      flushQueryId: 'flush-1',
      database: 'db',
      table: 'local_table',
    });
    expect(topology.executionFlow.map(event => event.kind)).toContain('async_insert_buffered');
    expect(adapter.queries.some(sql => sql.includes('system.asynchronous_insert_log'))).toBe(true);
    expect(adapter.queries.some(sql => sql.includes("query_id IN ('flush-1')"))).toBe(true);
  });
});
