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

class RoutingMockAdapter implements IClickHouseAdapter {
  public queries: string[] = [];

  constructor(
    private readonly respond: (
      sql: string,
    ) => Record<string, unknown>[] | Promise<Record<string, unknown>[]>,
  ) {}

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    return await this.respond(sql) as T[];
  }
}

const topologyExecutionRow = {
  query_id: 'query-root',
  initial_query_id: 'query-root',
  is_initial_query: 1,
  normalized_query_hash: '123',
  hostname: 'node-a',
  query_kind: 'Select',
  query_start_time_microseconds: '2026-06-18 12:00:00.000000',
  query_duration_ms: 10,
  memory_usage: 1000,
  read_rows: 10,
  read_bytes: 100,
  tables: ['db.table'],
  query_preview: 'SELECT * FROM db.table',
  ProfileEvents: {},
};

describe('QueryAnalyzer running queries', () => {
  it('applies a sanitized server-side result limit', async () => {
    const adapter = new MockAdapter();
    const analyzer = new QueryAnalyzer(adapter);

    await analyzer.getRunningQueries(100.9);

    expect(adapter.queries).toHaveLength(1);
    expect(adapter.queries[0]).toContain('FROM {{cluster_aware:system.processes}}');
    expect(adapter.queries[0]).toContain('ORDER BY elapsed DESC');
    expect(adapter.queries[0]).toContain('LIMIT 100');
  });
});

describe('QueryAnalyzer filter values', () => {
  it('uses query-row server identities for hostname suggestions', async () => {
    const adapter = new MockAdapter([
      { hostname: 'node-1.cluster.local' },
      { hostname: 'node-2' },
    ]);
    const analyzer = new QueryAnalyzer(adapter);

    const values = await analyzer.getDistinctFilterValues('hostname');

    expect(values).toEqual(['node-1', 'node-2']);
    expect(adapter.queries).toHaveLength(1);
    expect(adapter.queries[0]).toContain('SELECT DISTINCT hostName() AS hostname');
    expect(adapter.queries[0]).toContain('FROM {{cluster_aware:system.one}}');
    expect(adapter.queries[0]).not.toContain('system.clusters');
    expect(adapter.queries[0]).toContain('/* source:TraceHouse:Queries:filterValues */');
  });
});

describe('QueryAnalyzer UTC history bounds', () => {
  it('keeps offset conversion explicit in generated ClickHouse SQL', async () => {
    const adapter = new MockAdapter();
    const analyzer = new QueryAnalyzer(adapter);

    await analyzer.getQueryHistory({
      start_date: '2026-07-26',
      start_time: '2026-07-27T14:13:00+01:00',
      end_time: '2026-07-27T16:05:00+02:00',
    });

    expect(adapter.queries[0]).toContain(
      "event_time >= toDateTime('2026-07-27 13:13:00', 'UTC')",
    );
    expect(adapter.queries[0]).toContain(
      "event_time <= toDateTime('2026-07-27 14:05:00', 'UTC')",
    );
    expect(adapter.queries[0]).toContain("event_date >= '2026-07-26'");
    expect(adapter.queries[0]).toContain(
      "type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')",
    );
  });

  it('ORs multiple categorical values in generated history SQL', async () => {
    const adapter = new MockAdapter();
    const analyzer = new QueryAnalyzer(adapter);

    await analyzer.getQueryHistory({
      start_date: '2026-07-26',
      start_time: '2026-07-27T13:00:00Z',
      end_time: '2026-07-27T14:00:00Z',
      user: ['alice', "o'hara"],
      query_id: ['query-a', 'query-b'],
      query_kind: ['Select', 'Insert'],
      status: ['running', 'error'],
      exception_code: [60, 394],
      database: ['db_a', 'db_b'],
      table: ['table_a', 'table_b'],
      hostname: ['node-1', 'node-2'],
    });

    const sql = adapter.queries[0]!;
    expect(sql).toContain("user IN ('alice', 'o\\'hara')");
    expect(sql).toContain("query_id IN ('query-a', 'query-b')");
    expect(sql).toContain("query_kind IN ('Select', 'Insert')");
    expect(sql).toContain(
      "type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')",
    );
    expect(sql).toContain('exception_code IN (60, 394)');
    expect(sql).toContain("positionCaseInsensitive(x, 'db_a') > 0 OR positionCaseInsensitive(x, 'db_b') > 0");
    expect(sql).toContain("positionCaseInsensitive(x, 'table_a') > 0 OR positionCaseInsensitive(x, 'table_b') > 0");
    expect(sql).toContain("positionCaseInsensitive(hostName(), 'node-1') > 0 OR positionCaseInsensitive(hostName(), 'node-2') > 0");
  });

  it('maps errors rejected before execution into query history errors', async () => {
    const adapter = new MockAdapter([{
      query_id: 'rejected-query',
      type: 'ExceptionBeforeStart',
      query_start_time: '2026-07-27 13:30:00',
      query_duration_ms: 0,
      query: 'SELECT missing FROM unknown_table',
      exception: 'Unknown table',
      user: 'default',
      hostname: 'node-1',
    }]);
    const analyzer = new QueryAnalyzer(adapter);

    const history = await analyzer.getQueryHistory({
      start_date: '2026-07-27',
      start_time: '2026-07-27T13:00:00Z',
      end_time: '2026-07-27T14:00:00Z',
      status: ['error'],
    });

    expect(history).toHaveLength(1);
    expect(history[0]).toMatchObject({
      query_id: 'rejected-query',
      query_type: 'ExceptionBeforeStart',
      type: 'error',
      exception: 'Unknown table',
    });
    expect(adapter.queries[0]).toContain(
      "type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')",
    );
  });
});

describe('QueryAnalyzer child query lookup', () => {
  it('queries system.query_log directly without an analyzer-breaking wrapper', async () => {
    const adapter = new MockAdapter();
    const analyzer = new QueryAnalyzer(adapter);

    await analyzer.getSubQueries('parent-a', '2026-06-18');

    expect(adapter.queries).toHaveLength(1);
    expect(adapter.queries[0]).toContain('FROM {{cluster_aware:system.query_log}}');
    expect(adapter.queries[0]).not.toMatch(/FROM\s*\(\s*SELECT/i);
    expect(adapter.queries[0]).toContain('ORDER BY query_duration_ms DESC');
    expect(adapter.queries[0]).toContain('/* source:TraceHouse:Queries:subQueries */');
  });
});

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

  it('loads source Insert query_log rows when the active query is AsyncInsertFlush', async () => {
    const adapter = new MockAdapter();
    adapter.responseQueue = [
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
      [
        {
          event_time_microseconds: '2026-06-18 12:00:00.001000',
          flush_time_microseconds: '2026-06-18 12:00:00.002000',
          timeout_milliseconds: 200,
          hostname: 'node-a',
          query_id: 'insert-root',
          flush_query_id: 'flush-1',
          database: 'db',
          table: 'local_table',
          format: 'Native',
          data_kind: 'Preprocessed',
          status: 'Ok',
          exception: '',
          rows: 10,
          bytes: 100,
        },
      ],
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
    ];
    const analyzer = new QueryAnalyzer(adapter);

    const topology = await analyzer.getDistributedTopology('flush-1', '2026-06-18');

    expect(topology.nodes.map(node => [node.queryId, node.role])).toEqual([
      ['insert-root', 'insert_client'],
      ['flush-1', 'async_insert_flush'],
    ]);
    expect(topology.asyncInsertLinks[0]).toMatchObject({
      queryId: 'insert-root',
      flushQueryId: 'flush-1',
      format: 'Native',
      dataKind: 'Preprocessed',
      timeoutMilliseconds: 200,
    });
    expect(adapter.queries.some(sql => sql.includes("query_id IN ('insert-root')"))).toBe(true);
  });
});

describe('QueryAnalyzer distributed topology processor compatibility', () => {
  it('uses the full processor projection only when every host has plan-step columns', async () => {
    const adapter = new RoutingMockAdapter((sql) => {
      if (sql.includes('distributedTopologyExecutions')) return [topologyExecutionRow];
      if (sql.includes('distributedTopologyProcessors')) {
        return [{
          query_id: 'query-root',
          initial_query_id: 'query-root',
          hostname: 'node-a',
          plan_step_name: 'ReadFromStorage',
          plan_step_description: 'Read db.table',
          processor_name: 'ReadFromMergeTree',
        }];
      }
      return [];
    });
    const analyzer = new QueryAnalyzer(adapter);

    const topology = await analyzer.getDistributedTopology(
      'query-root',
      '2026-06-18',
      {
        mode: 'full',
        reason: 'full_schema',
        message: 'Processor names and plan-step metadata are available on every queried host.',
      },
    );

    const processorSql = adapter.queries.find(sql =>
      sql.includes('/* source:TraceHouse:Queries:distributedTopologyProcessors */'),
    );
    expect(adapter.queries.some(sql =>
      sql.includes('distributedTopologyProcessorCapabilities'),
    )).toBe(false);
    expect(processorSql).toContain('plan_step_name');
    expect(processorSql).toContain('plan_step_description');
    expect(processorSql).not.toContain("'' AS plan_step_name");
    expect(topology.processorProfileCompatibility).toMatchObject({
      mode: 'full',
      reason: 'full_schema',
    });
    expect(topology.capabilities.processorsProfileLog).toBe(true);
  });

  it('uses a legacy projection without referencing unsupported plan-step columns', async () => {
    const adapter = new RoutingMockAdapter((sql) => {
      if (sql.includes('distributedTopologyExecutions')) return [topologyExecutionRow];
      if (sql.includes('distributedTopologyProcessors')) {
        return [{
          query_id: 'query-root',
          initial_query_id: 'query-root',
          hostname: 'node-a',
          plan_step_name: '',
          plan_step_description: '',
          processor_name: 'ReadFromMergeTree',
        }];
      }
      return [];
    });
    const analyzer = new QueryAnalyzer(adapter);

    const topology = await analyzer.getDistributedTopology(
      'query-root',
      '2026-06-18',
      {
        mode: 'legacy',
        reason: 'legacy_schema',
        message: 'Processor plan-step metadata is unavailable on this ClickHouse schema; topology uses processor names and query_log/ProfileEvents.',
      },
    );

    const processorSql = adapter.queries.find(sql =>
      sql.includes('/* source:TraceHouse:Queries:distributedTopologyProcessors */'),
    );
    expect(processorSql).toContain("'' AS plan_step_name");
    expect(processorSql).toContain("'' AS plan_step_description");
    expect(processorSql).not.toContain('tracehouse:capability=processor_plan_step');
    expect(topology.processorProfileCompatibility).toMatchObject({
      mode: 'legacy',
      reason: 'legacy_schema',
    });
    expect(topology.capabilities.processorsProfileLog).toBe(true);
    expect(topology.decisions).toContainEqual(expect.objectContaining({
      code: 'legacy-processors-profile-log',
    }));
  });

  it('does not query processors_profile_log when any host lacks its base schema', async () => {
    const adapter = new RoutingMockAdapter((sql) => {
      if (sql.includes('distributedTopologyExecutions')) return [topologyExecutionRow];
      return [];
    });
    const analyzer = new QueryAnalyzer(adapter);

    const topology = await analyzer.getDistributedTopology(
      'query-root',
      '2026-06-18',
      {
        mode: 'unavailable',
        reason: 'missing_or_partial_schema',
        message: 'processors_profile_log is unavailable on one or more queried hosts; phase labels use query_log/ProfileEvents only.',
      },
    );

    expect(adapter.queries.some(sql =>
      sql.includes('/* source:TraceHouse:Queries:distributedTopologyProcessors */'),
    )).toBe(false);
    expect(topology.processorProfileCompatibility).toMatchObject({
      mode: 'unavailable',
      reason: 'missing_or_partial_schema',
    });
    expect(topology.capabilities.processorsProfileLog).toBe(false);
  });

  it('retains a connection-time probe failure instead of issuing a speculative processor query', async () => {
    const adapter = new RoutingMockAdapter((sql) => {
      if (sql.includes('distributedTopologyExecutions')) return [topologyExecutionRow];
      return [];
    });
    const analyzer = new QueryAnalyzer(adapter);

    const topology = await analyzer.getDistributedTopology(
      'query-root',
      '2026-06-18',
      {
        mode: 'unavailable',
        reason: 'schema_probe_failed',
        message: 'Processor-profile compatibility could not be determined at connection time; processor enrichment was not run.',
        detail: 'system.columns access denied',
      },
    );

    expect(adapter.queries.some(sql =>
      sql.includes('/* source:TraceHouse:Queries:distributedTopologyProcessors */'),
    )).toBe(false);
    expect(topology.processorProfileCompatibility).toEqual({
      mode: 'unavailable',
      reason: 'schema_probe_failed',
      message: 'Processor-profile compatibility could not be determined at connection time; processor enrichment was not run.',
      detail: 'system.columns access denied',
    });
  });

  it('retains an unexpected processor-query failure as structured degradation', async () => {
    const adapter = new RoutingMockAdapter((sql) => {
      if (sql.includes('distributedTopologyExecutions')) return [topologyExecutionRow];
      if (sql.includes('distributedTopologyProcessors')) {
        throw new Error('processor log query failed');
      }
      return [];
    });
    const analyzer = new QueryAnalyzer(adapter);

    const topology = await analyzer.getDistributedTopology(
      'query-root',
      '2026-06-18',
      {
        mode: 'full',
        reason: 'full_schema',
        message: 'Processor names and plan-step metadata are available on every queried host.',
      },
    );

    expect(topology.processorProfileCompatibility).toEqual({
      mode: 'unavailable',
      reason: 'query_failed',
      message: 'Processor-profile enrichment could not be loaded; topology uses query_log/ProfileEvents only.',
      detail: 'processor log query failed',
    });
    expect(topology.capabilities.processorsProfileLog).toBe(false);
  });
});
