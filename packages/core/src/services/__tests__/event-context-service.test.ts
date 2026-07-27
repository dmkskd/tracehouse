import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter, TaggedQuery } from '../../adapters/types.js';
import { EventContextService } from '../event-context-service.js';

class ContextAdapter implements IClickHouseAdapter {
  readonly queries: string[] = [];

  constructor(
    private readonly responder: (sql: string) => Record<string, unknown>[],
  ) {}

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    return this.responder(sql) as T[];
  }
}

const baseOptions = {
  eventTime: '2026-07-26T10:00:00.000Z',
  hostname: 'host-a',
  queryId: 'query-a',
  initialQueryId: 'query-a',
  windowSeconds: 300,
};

describe('EventContextService', () => {
  it('only queries capability-confirmed sources', async () => {
    const adapter = new ContextAdapter(sql => {
      if (sql.includes('system.query_log')) {
        return [{
          host: 'host-a',
          query_id: 'query-a',
          initial_query_id: 'query-a',
          start_time: '2026-07-26 09:59:55.000',
          end_time: '2026-07-26 10:00:01.000',
          query_duration_ms: 6000,
          memory_usage: 100,
          cpu_us: 200,
          read_rows: 3,
          read_bytes: 4,
          written_rows: 0,
          written_bytes: 0,
          status: 'QueryFinish',
          query: 'SELECT 1',
          is_event_query: 1,
        }];
      }
      return [];
    });
    const result = await new EventContextService(adapter).getContext({
      ...baseOptions,
      availableCapabilities: ['query_log'],
    });

    expect(adapter.queries).toHaveLength(1);
    expect(adapter.queries[0]).toContain('system.query_log');
    expect(adapter.queries[0]).toContain('source:TraceHouse:Events:contextWorkload');
    expect(adapter.queries[0]).toContain(
      "toDateTime64('2026-07-26 10:00:00.000', 3, 'UTC')",
    );
    expect(result.workload.status).toBe('loaded');
    expect(result.workload.data[0]).toMatchObject({
      query_id: 'query-a',
      is_event_query: true,
    });
    expect(result.metrics.status).toBe('unavailable');
    expect(result.logs.status).toBe('unavailable');
  });

  it('uses the preceding metric sample as the event snapshot', async () => {
    const adapter = new ContextAdapter(sql => {
      if (!sql.includes('system.metric_log')) return [];
      return [
        {
          host: 'host-a',
          sample_time: '2026-07-26 09:59:55',
          memory_usage: 100,
          active_queries: 2,
          active_merges: 1,
          cpu_cores: 0.5,
        },
        {
          host: 'host-a',
          sample_time: '2026-07-26 10:00:05',
          memory_usage: 200,
          active_queries: 4,
          active_merges: 0,
          cpu_cores: 1,
        },
      ];
    });
    const result = await new EventContextService(adapter).getContext({
      ...baseOptions,
      availableCapabilities: ['metric_log'],
    });

    expect(result.metrics.snapshots).toHaveLength(1);
    expect(result.metrics.snapshots[0]).toMatchObject({
      hostname: 'host-a',
      memory_usage: 100,
      sample_age_ms: 5000,
    });
    expect(result.metrics.data).toHaveLength(2);
  });

  it('does not discard successful sources when another source fails', async () => {
    const adapter = new ContextAdapter(sql => {
      if (sql.includes('system.text_log')) throw new Error('text_log denied');
      if (sql.includes('system.metric_log')) {
        return [{
          host: 'host-a',
          sample_time: '2026-07-26 10:00:00',
          memory_usage: 100,
          active_queries: 1,
          active_merges: 0,
          cpu_cores: 0.2,
        }];
      }
      return [];
    });
    const result = await new EventContextService(adapter).getContext({
      ...baseOptions,
      availableCapabilities: ['query_log', 'metric_log', 'text_log'],
    });

    expect(result.workload.status).toBe('loaded');
    expect(result.metrics.status).toBe('loaded');
    expect(result.logs.status).toBe('failed');
    expect(result.logs.detail).toContain('text_log denied');
  });

  it('bounds the context window independently around the event', async () => {
    const adapter = new ContextAdapter(() => []);
    const result = await new EventContextService(adapter).getContext({
      ...baseOptions,
      windowSeconds: 60,
      availableCapabilities: [],
    });

    expect(result.window_start).toBe('2026-07-26T09:59:00.000Z');
    expect(result.window_end).toBe('2026-07-26T10:01:00.000Z');
    expect(adapter.queries).toHaveLength(0);
  });
});
