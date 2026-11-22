import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { TimelineService } from '../timeline-service.js';
import { ACTIVE_QUERIES, ACTIVE_QUERIES_TRACKED, RUNNING_QUERIES_TIMELINE } from '../../queries/timeline-queries.js';

describe('timeline query table metadata', () => {
  it('fetches query-log table sets without asking system.processes for an unavailable field', () => {
    expect(ACTIVE_QUERIES).toMatch(/query_kind,\s+tables,/);
    expect(ACTIVE_QUERIES_TRACKED).toMatch(/query_kind,\s+tables,/);
    expect(RUNNING_QUERIES_TIMELINE).not.toMatch(/\btables\b/);
  });
  it('preserves reported table arrays and leaves unavailable metadata unknown', async () => {
    const adapter: IClickHouseAdapter = {
      async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
        if (!sql.includes('query_short') || !sql.includes('system.query_log')) return [];
        return [
          { query_id: 'with-tables', tables: ['db.destination', 'db.source'] },
          { query_id: 'unknown' },
        ].map(row => ({ ...row, host: 'ch-01', user: 'default', query_short: 'INSERT INTO db.destination SELECT * FROM db.source',
          qst: '2026-11-17 11:30:00', qet: '2026-11-17 11:30:02', memory_usage: 1e8, query_duration_ms: 2000, query_kind: 'Insert' })) as unknown as T[];
      },
    };
    const data = await new TimelineService(adapter).getTimeline({ timestamp: new Date('2026-11-17T11:30:00Z'), windowSeconds: 60, includeRunning: false });
    expect(data.queries.find(q => q.query_id === 'with-tables')?.tables).toEqual(['db.destination', 'db.source']);
    expect(data.queries.find(q => q.query_id === 'unknown')).toBeDefined();
    expect(data.queries.find(q => q.query_id === 'unknown')?.tables).toBeUndefined();
  });
});
